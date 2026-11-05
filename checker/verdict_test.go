package main

import (
	"strings"
	"testing"
)

func results(failing ...string) []Result {
	var rs []Result
	for _, id := range []string{"I1", "I2", "I3", "I4", "I5", "I6", "I7", "I8", "I9", "I11"} {
		pass := true
		for _, f := range failing {
			if f == id {
				pass = false
			}
		}
		rs = append(rs, Result{ID: id, Pass: pass})
	}
	return rs
}

func TestVerdict(t *testing.T) {
	cases := []struct {
		mode    string
		failing []string
		ok      bool
		says    string
	}{
		{"correct", nil, true, "correct passes every invariant"},
		{"correct", []string{"I9"}, false, "correct fails I9"},
		{"no-idempotency", []string{"I4", "I6", "I7"}, true, "no-idempotency fails I4, I6, I7"},
		{"no-idempotency", []string{"I4", "I7"}, false, "passes I6"},
		{"dual-write", []string{"I8", "I9"}, true, "dual-write fails I8, I9"},
		{"dual-write", nil, false, "must fail I8, I9 but passes I8, I9"},
		{"unordered-locks", []string{"I11"}, true, "unordered-locks fails I11"},
		{"read-modify-write", []string{"I2", "I3"}, true, "read-modify-write fails I2, I3, and must fail I2"},
		{"read-modify-write", []string{"I3"}, false, "passes I2"},
	}
	for _, c := range cases {
		ok, says := Verdict(c.mode, results(c.failing...))
		if ok != c.ok || !strings.Contains(says, c.says) {
			t.Errorf("%s with %v: got %v %q", c.mode, c.failing, ok, says)
		}
	}
}

func TestKnownMode(t *testing.T) {
	for mode := range mustFail {
		if err := knownMode(mode); err != nil {
			t.Error(err)
		}
	}
	if err := knownMode("corect"); err == nil {
		t.Error("expected an error for a typo")
	}
}
