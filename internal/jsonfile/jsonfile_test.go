package jsonfile_test

import (
	"errors"
	"strings"
	"testing"

	"github.com/nuggocto/bunpo/internal/jsonfile"
)

type sample struct {
	Version int64             `json:"version"`
	Values  map[string]string `json:"values"`
}

func (s *sample) FileVersion() int64 { return s.Version }

func TestDecode(t *testing.T) {
	var got sample
	data := []byte(`{"version":1,"values":{"b":"2","a":"1"}}`)
	if err := jsonfile.Decode("sample", data, 1024, 1, &got); err != nil {
		t.Fatal(err)
	}

	if got.Version != 1 || got.Values["a"] != "1" || got.Values["b"] != "2" {
		t.Fatalf("got = %+v", got)
	}
}

func TestDecodeRejects(t *testing.T) {
	tests := []struct {
		name    string
		data    []byte
		max     int
		wantErr error
		wantMsg string
	}{
		{"over size limit", []byte(`{"version":1,"values":{}}`), 10, jsonfile.ErrInvalid, "byte limit"},
		// Version is checked before unknown members so newer releases fail clearly.
		{"newer version with unknown member", []byte(`{"version":2,"added":true}`), 1024, jsonfile.ErrUnsupportedVersion, "version 2"},
		{"null document", []byte(`null`), 1024, jsonfile.ErrUnsupportedVersion, "version 0"},
		{"unknown member", []byte(`{"version":1,"values":{},"extra":1}`), 1024, jsonfile.ErrInvalid, "extra"},
		{"duplicate member", []byte(`{"version":1,"values":{"a":"1","a":"2"}}`), 1024, jsonfile.ErrInvalid, "duplicate"},
		{"invalid UTF-8", []byte("{\"version\":1,\"values\":{\"a\xff\":\"1\"}}"), 1024, jsonfile.ErrInvalid, ""},
		{"trailing garbage", []byte(`{"version":1,"values":{}} trailing`), 1024, jsonfile.ErrInvalid, ""},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var got sample
			err := jsonfile.Decode("sample", tc.data, tc.max, 1, &got)
			if !errors.Is(err, tc.wantErr) {
				t.Fatalf("error = %v; want %v", err, tc.wantErr)
			}

			if !strings.Contains(err.Error(), tc.wantMsg) {
				t.Fatalf("error %q does not mention %q", err, tc.wantMsg)
			}
		})
	}
}

func TestEncode(t *testing.T) {
	v := sample{Version: 1, Values: map[string]string{"b": "2", "a": "1"}}
	got, err := jsonfile.Encode("sample", v, 1024)
	if err != nil {
		t.Fatal(err)
	}

	want := `{"version":1,"values":{"a":"1","b":"2"}}`
	if string(got) != want {
		t.Fatalf("Encode = %s; want %s", got, want)
	}

	if _, err := jsonfile.Encode("sample", v, 10); !errors.Is(err, jsonfile.ErrInvalid) {
		t.Fatalf("oversized Encode error = %v; want %v", err, jsonfile.ErrInvalid)
	}
}
