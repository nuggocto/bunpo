// Package junit reads test reports into per-file totals: JUnit XML, or a
// two-column format for runners without one. Reports are untrusted, so every
// read is bounded in size, nesting and test cases.
package junit

import (
	"bufio"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"math/big"
	"strconv"
	"strings"

	"github.com/nuggocto/bunpo/internal/timing"
)

const (
	MaxReportBytes = 64 << 20
	MaxDepth       = 32 // real reports nest four levels
	MaxCases       = 2_000_000
)

var (
	ErrInvalidReport = errors.New("invalid test report")
	ErrNoFile        = errors.New("test case names no file")
)

// Source says where a JUnit report keeps each test case's file path.
type Source int

const (
	// FileAttribute reads the case's file attribute, or the nearest
	// enclosing suite's. pytest (xunit1), Jest, Vitest and RSpec write it.
	FileAttribute Source = iota + 1

	// Classname reads the classname attribute, where Playwright writes the
	// path relative to its testDir.
	Classname

	// SuiteName reads the nearest enclosing suite's name.
	SuiteName
)

// File is the total for one test file. Seconds is exact, so the total is
// rounded only once, by MS. Skipped counts test cases with a direct <skipped>
// child and does not set Failed.
type File struct {
	Seconds *big.Rat
	Cases   int
	Skipped int
	Failed  bool
}

// Report holds totals keyed by the path exactly as the report wrote it.
type Report struct {
	Files map[string]File
}

var maxSeconds = big.NewRat(timing.MaxDurationMS, 1000)

// MS rounds the total to whole milliseconds, half up.
func (f File) MS() int64 {
	if f.Seconds == nil {
		return 0
	}

	ms := new(big.Rat).Mul(f.Seconds, big.NewRat(1000, 1))
	q, r := new(big.Int).QuoRem(ms.Num(), ms.Denom(), new(big.Int))
	if r.Lsh(r, 1).Cmp(ms.Denom()) >= 0 {
		q.Add(q, big.NewInt(1))
	}
	return q.Int64()
}

// Add combines two totals. It reports false when the sum would pass
// timing.MaxDurationMS.
func (f File) Add(g File) (File, bool) {
	sum := new(big.Rat)
	for _, s := range []*big.Rat{f.Seconds, g.Seconds} {
		if s != nil {
			sum.Add(sum, s)
		}
	}

	if sum.Cmp(maxSeconds) > 0 {
		return File{}, false
	}
	return File{Seconds: sum, Cases: f.Cases + g.Cases, Skipped: f.Skipped + g.Skipped, Failed: f.Failed || g.Failed}, true
}

func Read(r io.Reader, src Source) (Report, error) {
	return read(r, src, MaxReportBytes)
}

// read takes the size limit as a parameter so tests can reach it cheaply.
func read(r io.Reader, src Source, maxBytes int64) (Report, error) {
	if src != FileAttribute && src != Classname && src != SuiteName {
		return Report{}, fmt.Errorf("unknown file source %d", src)
	}

	p := &parser{src: src, report: Report{Files: map[string]File{}}}
	dec := xml.NewDecoder(&limitReader{r: r, max: maxBytes})

	for {
		tok, err := dec.Token()
		if err == io.EOF {
			break
		}
		if err != nil {
			return Report{}, fmt.Errorf("%w: %v", ErrInvalidReport, err)
		}

		switch t := tok.(type) {
		case xml.StartElement:
			err = p.start(t)

		case xml.EndElement:
			err = p.end()

		// Text outside the root means the file is not one JUnit document.
		case xml.CharData:
			if p.depth == 0 && len(strings.TrimSpace(string(t))) > 0 {
				err = fmt.Errorf("%w: text outside the root element", ErrInvalidReport)
			}
		}

		if err != nil {
			return Report{}, err
		}
	}

	if !p.sawRoot {
		return Report{}, fmt.Errorf("%w: no root element; the runner may have failed before writing it", ErrInvalidReport)
	}
	return p.report, nil
}

type suite struct {
	depth      int
	file, name string
}

type parser struct {
	src     Source
	report  Report
	suites  []suite // enclosing testsuites, innermost last
	depth   int
	cases   int
	sawRoot bool

	// The open testcase, if inCase.
	inCase  bool
	caseAt  int
	file    string
	seconds *big.Rat
	failed  bool
	skipped bool
}

func (p *parser) start(t xml.StartElement) error {
	p.depth++
	if p.depth > MaxDepth {
		return fmt.Errorf("%w: elements nest deeper than %d levels", ErrInvalidReport, MaxDepth)
	}

	name := t.Name.Local

	switch {
	case p.depth == 1 && p.sawRoot:
		return fmt.Errorf("%w: more than one root element", ErrInvalidReport)

	case p.depth == 1 && name != "testsuites" && name != "testsuite":
		return fmt.Errorf("%w: root element is <%s>, not <testsuites> or <testsuite>", ErrInvalidReport, name)

	case p.depth == 1:
		p.sawRoot = true

	// Only a direct child counts: <failure> or <skipped> inside <system-out> is text.
	case p.inCase:
		if p.depth == p.caseAt+1 {
			switch name {
			case "failure", "error":
				p.failed = true
			case "skipped":
				p.skipped = true
			}
		}
		return nil
	}

	switch name {
	case "testsuite":
		p.suites = append(p.suites, suite{depth: p.depth, file: attr(t, "file"), name: attr(t, "name")})

	case "testcase":
		return p.startCase(t)
	}
	return nil
}

func (p *parser) startCase(t xml.StartElement) error {
	p.cases++
	if p.cases > MaxCases {
		return fmt.Errorf("%w: more than %d test cases", ErrInvalidReport, MaxCases)
	}

	seconds, err := parseSeconds(attr(t, "time"))
	if err != nil {
		return fmt.Errorf("%w: test case %q: %v", ErrInvalidReport, attr(t, "name"), err)
	}

	// A case without a file cannot be left out: its failure would go with it.
	file := p.caseFile(t)
	if file == "" {
		return fmt.Errorf("%w: %q", ErrNoFile, attr(t, "name"))
	}

	p.inCase, p.caseAt, p.file, p.seconds, p.failed, p.skipped = true, p.depth, file, seconds, false, false
	return nil
}

func (p *parser) caseFile(t xml.StartElement) string {
	switch p.src {
	case Classname:
		return attr(t, "classname")

	case SuiteName:
		if len(p.suites) == 0 {
			return ""
		}
		return p.suites[len(p.suites)-1].name

	default:
		file := attr(t, "file")
		for i := len(p.suites) - 1; file == "" && i >= 0; i-- {
			file = p.suites[i].file
		}
		return file
	}
}

func (p *parser) end() error {
	defer func() { p.depth-- }()

	if p.inCase && p.depth == p.caseAt {
		p.inCase = false
		skipped := 0
		if p.skipped {
			skipped = 1
		}
		if err := p.report.add(p.file, File{Seconds: p.seconds, Cases: 1, Skipped: skipped, Failed: p.failed}); err != nil {
			return err
		}
	}

	if n := len(p.suites); n > 0 && p.suites[n-1].depth == p.depth {
		p.suites = p.suites[:n-1]
	}
	return nil
}

// ReadLines reads one file per line: a path, whitespace, and either whole
// milliseconds or "-" for a failed file whose time should not be recorded.
// Splitting at the last whitespace keeps paths with spaces, and repeated
// paths add up.
func ReadLines(r io.Reader) (Report, error) {
	return readLines(r, MaxReportBytes, MaxCases)
}

// readLines takes the bounds as parameters so tests can reach them cheaply.
func readLines(r io.Reader, maxBytes int64, maxLines int) (Report, error) {
	report := Report{Files: map[string]File{}}

	sc := bufio.NewScanner(&limitReader{r: r, max: maxBytes})
	sc.Buffer(make([]byte, 0, 64<<10), 64<<10)

	for line := 1; sc.Scan(); line++ {
		if line > maxLines {
			return Report{}, fmt.Errorf("%w: more than %d lines", ErrInvalidReport, maxLines)
		}

		text := strings.TrimRight(sc.Text(), "\r")
		if strings.TrimSpace(text) == "" {
			continue
		}

		i := strings.LastIndexAny(text, " \t")
		path := strings.TrimRight(text[:max(i, 0)], " \t")
		if i < 0 || path == "" {
			return Report{}, fmt.Errorf("%w: line %d: want a path and milliseconds", ErrInvalidReport, line)
		}

		field := text[i+1:]
		if field == "-" {
			if err := report.add(path, File{Cases: 1, Failed: true}); err != nil {
				return Report{}, err
			}
			continue
		}

		ms, err := strconv.ParseInt(field, 10, 64)
		if err != nil || !allDigits(field) || ms > timing.MaxDurationMS {
			return Report{}, fmt.Errorf("%w: line %d: %q is not whole milliseconds from 0 to %d (or \"-\" for a failed file)", ErrInvalidReport, line, field, timing.MaxDurationMS)
		}

		if err := report.add(path, File{Seconds: big.NewRat(ms, 1000), Cases: 1}); err != nil {
			return Report{}, err
		}
	}

	if err := sc.Err(); err != nil {
		return Report{}, fmt.Errorf("%w: %v", ErrInvalidReport, err)
	}
	return report, nil
}

func (r *Report) add(path string, f File) error {
	sum, ok := r.Files[path].Add(f)
	if !ok {
		return fmt.Errorf("%w: file %q adds up to more than %d ms", ErrInvalidReport, path, timing.MaxDurationMS)
	}

	r.Files[path] = sum
	return nil
}

// parseSeconds reads decimal seconds exactly, without floats. Exponents
// appear because JavaScript writes tiny numbers as 1e-7. An empty value is 0:
// Playwright omits it on skipped cases.
func parseSeconds(s string) (*big.Rat, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return new(big.Rat), nil
	}
	if len(s) > 40 {
		return nil, fmt.Errorf("time %q is too long", s)
	}

	mantissa, exp := s, 0
	if i := strings.IndexAny(s, "eE"); i >= 0 {
		e, err := strconv.Atoi(s[i+1:])
		if err != nil || e < -40 || e > 40 {
			return nil, fmt.Errorf("time %q has an invalid exponent", s)
		}
		mantissa, exp = s[:i], e
	}

	whole, frac, _ := strings.Cut(mantissa, ".")
	digits := whole + frac
	if !allDigits(digits) {
		return nil, fmt.Errorf("time %q is not a nonnegative decimal number", s)
	}

	// The value is digits × 10^(exp - len(frac)).
	n, _ := new(big.Int).SetString(digits, 10)
	value := new(big.Rat).SetInt(n)

	if shift := exp - len(frac); shift >= 0 {
		value.Mul(value, new(big.Rat).SetInt(pow10(shift)))
	} else {
		value.Quo(value, new(big.Rat).SetInt(pow10(-shift)))
	}

	if value.Cmp(maxSeconds) > 0 {
		return nil, fmt.Errorf("time %q is longer than %d ms", s, timing.MaxDurationMS)
	}
	return value, nil
}

func pow10(n int) *big.Int {
	return new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(n)), nil)
}

func allDigits(s string) bool {
	if s == "" {
		return false
	}

	for i := range len(s) {
		if s[i] < '0' || s[i] > '9' {
			return false
		}
	}
	return true
}

func attr(t xml.StartElement, name string) string {
	for _, a := range t.Attr {
		if a.Name.Local == name {
			return a.Value
		}
	}
	return ""
}

var errTooLarge = errors.New("report is over the size limit")

// limitReader reports an overflow instead of ending the input early, which
// would surface as a confusing XML syntax error.
type limitReader struct {
	r   io.Reader
	n   int64
	max int64
}

func (l *limitReader) Read(p []byte) (int, error) {
	rem := l.max - l.n
	if rem < 0 {
		return 0, fmt.Errorf("%w of %d bytes", errTooLarge, l.max)
	}
	if int64(len(p)) > rem+1 {
		p = p[:rem+1]
	}

	n, err := l.r.Read(p)
	l.n += int64(n)

	if l.n > l.max {
		return 0, fmt.Errorf("%w of %d bytes", errTooLarge, l.max)
	}
	return n, err
}
