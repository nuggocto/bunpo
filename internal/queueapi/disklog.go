package queueapi

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json/v2"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"log/slog"
	"os"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"sync"

	"github.com/nuggocto/bunpo/internal/queue"
)

const (
	logHeader         = "bunpo-queue-log 1\n"
	MaxLogRecordBytes = 8 << 20
)

var crc32cTable = crc32.MakeTable(crc32.Castagnoli)

// LogFS is the minimal filesystem surface DiskLog needs so production (os.Root)
// and deterministic simulation tests run the exact same log code.
type LogFS interface {
	OpenFile(name string, flag int, perm os.FileMode) (LogFile, error)
	ReadDir() ([]string, error)
	Remove(name string) error
	SyncDir() error
}

// LogFile is one open file returned by LogFS.OpenFile.
type LogFile interface {
	io.ReadWriteCloser
	StatSize() (int64, error)
	Truncate(size int64) error
	Sync() error
}

// OSLogFS implements LogFS over an os.Root directory (0700 dir, 0600 files).
type OSLogFS struct {
	root *os.Root
}

// OpenOSLogFS creates dir (0700) if needed and opens it via os.OpenRoot.
func OpenOSLogFS(dir string) (*OSLogFS, error) {
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, fmt.Errorf("create data dir %q: %w", dir, err)
	}

	root, err := os.OpenRoot(dir)
	if err != nil {
		return nil, fmt.Errorf("open data dir %q: %w", dir, err)
	}

	return &OSLogFS{root: root}, nil
}

func (o *OSLogFS) Close() error             { return o.root.Close() }
func (o *OSLogFS) Remove(name string) error { return o.root.Remove(name) }

func (o *OSLogFS) OpenFile(name string, flag int, perm os.FileMode) (LogFile, error) {
	f, err := o.root.OpenFile(name, flag, perm)
	if err != nil {
		return nil, err
	}

	return osLogFile{f}, nil
}

func (o *OSLogFS) ReadDir() ([]string, error) {
	d, err := o.root.Open(".")
	if err != nil {
		return nil, err
	}
	defer d.Close()
	return d.Readdirnames(-1)
}

func (o *OSLogFS) SyncDir() error {
	if runtime.GOOS == "windows" {
		return nil // Windows rejects FlushFileBuffers on directory handles.
	}

	d, err := o.root.Open(".")
	if err != nil {
		return err
	}

	return errors.Join(d.Sync(), d.Close())
}

type osLogFile struct{ *os.File }

func (o osLogFile) StatSize() (int64, error) {
	fi, err := o.Stat()
	if err != nil {
		return 0, err
	}

	return fi.Size(), nil
}

// BuildFileName returns the on-disk filename for buildID (lowercase hex SHA-256).
func BuildFileName(buildID string) string {
	sum := sha256.Sum256([]byte(buildID))
	return hex.EncodeToString(sum[:])
}

// LoadResult summarizes builds recovered from disk on startup.
type LoadResult struct {
	Builds    []*queue.State
	Damaged   []string
	Truncated int
	Removed   int
}

// DiskLog persists per-build event logs as CRC-32C-framed JSON-array records.
type DiskLog struct {
	fs     LogFS
	logger *slog.Logger
	mu     sync.Mutex
	sizes  map[string]int64
}

// NewDiskLog constructs a DiskLog backed by fs.
func NewDiskLog(fs LogFS, logger *slog.Logger) *DiskLog {
	if logger == nil {
		logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	}

	return &DiskLog{
		fs:     fs,
		logger: logger,
		sizes:  make(map[string]int64),
	}
}

func encodeLogRecord(events []queue.Event) ([]byte, error) {
	payload, err := json.Marshal(events, json.Deterministic(true))
	if err != nil {
		return nil, fmt.Errorf("encode log record: %w", err)
	}

	if bytes.IndexByte(payload, '\n') >= 0 {
		return nil, errors.New("encode log record: payload contains newline")
	}

	crc := crc32.Checksum(payload, crc32cTable)
	rec := make([]byte, 0, 10+len(payload))
	rec = fmt.Appendf(rec, "%08x ", crc)
	rec = append(rec, payload...)
	rec = append(rec, '\n')
	if len(rec) > MaxLogRecordBytes {
		return nil, fmt.Errorf("log record size %d exceeds %d", len(rec), MaxLogRecordBytes)
	}

	return rec, nil
}

func decodeLogRecord(line []byte) ([]queue.Event, error) {
	if len(line) < 11 || line[len(line)-1] != '\n' || line[8] != ' ' {
		return nil, errors.New("malformed record framing")
	}

	payload := line[9 : len(line)-1]
	wantCRC, err := strconv.ParseUint(string(line[:8]), 16, 32)
	if err != nil || uint64(crc32.Checksum(payload, crc32cTable)) != wantCRC {
		return nil, errors.New("crc32c mismatch")
	}

	var events []queue.Event
	if err := json.Unmarshal(payload, &events, json.RejectUnknownMembers(true)); err != nil {
		return nil, fmt.Errorf("decode events: %w", err)
	}

	if len(events) == 0 {
		return nil, errors.New("empty event record")
	}

	reencoded, err := json.Marshal(events, json.Deterministic(true))
	if err != nil || !bytes.Equal(reencoded, payload) {
		return nil, errors.New("record failed strict round-trip check")
	}

	return events, nil
}

// Append writes events as one atomic CRC-32C-framed record and fsyncs before returning.
func (l *DiskLog) Append(_ context.Context, buildID string, events []queue.Event) error {
	if len(events) == 0 {
		return nil
	}

	rec, err := encodeLogRecord(events)
	if err != nil {
		return fmt.Errorf("%w (%w)", err, ErrAppendUnwritten)
	}

	name := BuildFileName(buildID)
	l.mu.Lock()
	expectedSize, exists := l.sizes[buildID]
	l.mu.Unlock()

	var (
		flag    int
		payload []byte
	)
	if exists {
		flag = os.O_WRONLY | os.O_APPEND
		payload = rec
	} else {
		flag = os.O_CREATE | os.O_EXCL | os.O_WRONLY
		payload = append(append(make([]byte, 0, len(logHeader)+len(rec)), logHeader...), rec...)
	}

	f, err := l.fs.OpenFile(name, flag, 0o600)
	if err != nil {
		if (!exists && errors.Is(err, os.ErrExist)) || (exists && errors.Is(err, os.ErrNotExist)) {
			return fmt.Errorf("unexpected build log state %s: %w", name, err)
		}

		return fmt.Errorf("open build log: %w (%w)", err, ErrAppendUnwritten)
	}

	if exists {
		sz, sErr := f.StatSize()
		if sErr != nil {
			_ = f.Close()
			return fmt.Errorf("stat build log: %w (%w)", sErr, ErrAppendUnwritten)
		}

		if sz != expectedSize {
			_ = f.Close()
			return fmt.Errorf("build log %s size %d != expected %d", name, sz, expectedSize)
		}
	}

	if _, err := f.Write(payload); err != nil {
		var rollbackErr error
		if !exists {
			rollbackErr = errors.Join(f.Close(), l.fs.Remove(name))
		} else {
			rollbackErr = errors.Join(f.Truncate(expectedSize), f.Sync(), f.Close())
		}

		if rollbackErr == nil {
			return fmt.Errorf("write build log: %w (%w)", err, ErrAppendUnwritten)
		}

		return fmt.Errorf("write build log and rollback failed: %w", err)
	}

	syncErr := errors.Join(f.Sync(), f.Close())
	if !exists {
		syncErr = errors.Join(syncErr, l.fs.SyncDir())
	}

	if syncErr != nil {
		return fmt.Errorf("sync build log: %w", syncErr)
	}

	l.mu.Lock()
	l.sizes[buildID] = expectedSize + int64(len(payload))
	l.mu.Unlock()
	return nil
}

// Remove deletes buildID's log file and fsyncs the directory before clearing its tracked size.
func (l *DiskLog) Remove(_ context.Context, buildID string) error {
	if err := l.removeAndSync(BuildFileName(buildID)); err != nil {
		return err
	}

	l.mu.Lock()
	delete(l.sizes, buildID)
	l.mu.Unlock()
	return nil
}

// Load streams every build log in the directory, removing empty/uninitialized
// files, truncating torn last records, quarantining damaged files untouched,
// and fsyncing every accepted file and the directory once before serving.
func (l *DiskLog) Load(lim queue.Limits) (LoadResult, error) {
	names, err := l.fs.ReadDir()
	if err != nil {
		return LoadResult{}, fmt.Errorf("read data dir: %w", err)
	}

	slices.Sort(names)

	var res LoadResult
	for _, name := range names {
		if len(name) != 64 || strings.Trim(name, "0123456789abcdef") != "" {
			l.logger.Warn("ignoring non-build entry in data dir", "name", name)
			continue
		}

		st, validOff, tornErr, dmgErr := l.readBuildFile(name, lim)
		if dmgErr == nil && st == nil {
			if err := l.removeAndSync(name); err != nil {
				dmgErr = fmt.Errorf("remove empty build log: %w", err)
			} else {
				res.Removed++
				continue
			}
		}

		if dmgErr == nil && tornErr != nil {
			l.logger.Warn("truncating torn tail record in build log", "file", name, "offset", validOff, "err", tornErr)

			if err := l.truncateAndSync(name, validOff); err != nil {
				dmgErr = fmt.Errorf("truncate torn tail: %w", err)
			} else {
				res.Truncated++
			}
		} else if dmgErr == nil {
			// Fsync accepted files that were not already fsynced by truncation,
			// so page-cache bytes from a prior process kill become durable
			// before the server answers requests from them.
			if err := l.syncFile(name); err != nil {
				dmgErr = fmt.Errorf("fsync loaded build log: %w", err)
			}
		}

		if dmgErr != nil {
			l.logger.Error("build log damaged; leaving file untouched", "file", name, "err", dmgErr)
			res.Damaged = append(res.Damaged, name)
			continue
		}

		l.mu.Lock()
		l.sizes[st.BuildID] = validOff
		l.mu.Unlock()
		res.Builds = append(res.Builds, st)
	}

	if err := l.fs.SyncDir(); err != nil {
		return LoadResult{}, fmt.Errorf("sync data dir after load: %w", err)
	}

	return res, nil
}

func (l *DiskLog) readBuildFile(name string, lim queue.Limits) (st *queue.State, validOff int64, tornErr, dmgErr error) {
	f, err := l.fs.OpenFile(name, os.O_RDONLY, 0)
	if err != nil {
		return nil, 0, nil, err
	}
	defer f.Close()

	br := bufio.NewReaderSize(f, 64*1024)
	hdr, _, moreHdr, tooLargeHdr, hdrIOErr := readBoundedLine(br, len(logHeader)+16)
	if hdrIOErr != nil {
		return nil, 0, nil, fmt.Errorf("read header: %w", hdrIOErr)
	}

	if tooLargeHdr {
		return nil, 0, nil, errors.New("header exceeds maximum length")
	}

	if !moreHdr && strings.HasPrefix(logHeader, string(hdr)) && string(hdr) != logHeader {
		return nil, 0, nil, nil
	}

	if string(hdr) != logHeader {
		return nil, 0, nil, fmt.Errorf("invalid header %q", string(hdr))
	}

	st = queue.NewState(lim)
	validOff = int64(len(logHeader))
	records := 0
	for {
		line, complete, more, tooLarge, ioErr := readBoundedLine(br, MaxLogRecordBytes)
		if ioErr != nil {
			return nil, 0, nil, fmt.Errorf("read record: %w", ioErr)
		}

		if len(line) == 0 && !complete {
			if records == 0 {
				return nil, 0, nil, nil
			}

			return st, validOff, nil, nil
		}

		var (
			events []queue.Event
			decErr error
		)
		switch {
		case tooLarge:
			return nil, 0, nil, errors.New("record exceeds max size")
		case !complete:
			decErr = errors.New("truncated record")
		default:
			events, decErr = decodeLogRecord(line)
		}

		if decErr != nil {
			if more {
				return nil, 0, nil, fmt.Errorf("corrupt record before EOF: %w", decErr)
			}

			if records == 0 {
				return nil, 0, nil, nil
			}

			return st, validOff, decErr, nil
		}

		if records == 0 {
			if events[0].Type != queue.EventInitialized || events[0].Initialized == nil || BuildFileName(events[0].Initialized.BuildID) != name {
				return nil, 0, nil, errors.New("first event does not initialize matching build ID")
			}
		}

		for _, ev := range events {
			if err := st.Apply(ev); err != nil {
				return nil, 0, nil, fmt.Errorf("apply %s: %w", ev.Type, err)
			}
		}

		records++
		validOff += int64(len(line))
	}
}

func (l *DiskLog) syncFile(name string) error {
	f, err := l.fs.OpenFile(name, os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}

	return errors.Join(f.Sync(), f.Close())
}

func (l *DiskLog) truncateAndSync(name string, size int64) error {
	f, err := l.fs.OpenFile(name, os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}

	if err := f.Truncate(size); err != nil {
		return errors.Join(err, f.Close())
	}

	return errors.Join(f.Sync(), f.Close())
}

func (l *DiskLog) removeAndSync(name string) error {
	if err := l.fs.Remove(name); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}

	return l.fs.SyncDir()
}

// readBoundedLine reads up to the next '\n' without exceeding maxBytes.
// Any I/O error other than io.EOF (from ReadSlice or Peek) is returned in ioErr
// so callers never mistake a read failure for end-of-file.
func readBoundedLine(br *bufio.Reader, maxBytes int) (line []byte, complete, moreData, tooLarge bool, ioErr error) {
	for {
		frag, rErr := br.ReadSlice('\n')
		if len(line)+len(frag) > maxBytes {
			for errors.Is(rErr, bufio.ErrBufferFull) {
				_, rErr = br.ReadSlice('\n')
			}

			if rErr != nil && !errors.Is(rErr, io.EOF) {
				return nil, false, false, false, rErr
			}

			if rErr == nil {
				if _, pErr := br.Peek(1); pErr == nil {
					moreData = true
				} else if !errors.Is(pErr, io.EOF) {
					return nil, false, false, false, pErr
				}
			}

			return line, false, moreData, true, nil
		}

		line = append(line, frag...)
		if rErr == nil {
			_, pErr := br.Peek(1)
			if pErr == nil {
				return line, true, true, false, nil
			}

			if errors.Is(pErr, io.EOF) {
				return line, true, false, false, nil
			}

			return nil, false, false, false, pErr
		}

		if errors.Is(rErr, io.EOF) {
			return line, false, false, false, nil
		}

		if !errors.Is(rErr, bufio.ErrBufferFull) {
			return nil, false, false, false, rErr
		}
	}
}
