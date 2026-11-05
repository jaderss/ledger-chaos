package main

import (
	"fmt"
	"slices"
	"strings"
)

var mustFail = map[string][]string{
	"correct":           {},
	"no-idempotency":    {"I4", "I6", "I7"},
	"dual-write":        {"I8", "I9"},
	"unordered-locks":   {"I11"},
	"read-modify-write": {"I2"},
}

func knownMode(mode string) error {
	if _, ok := mustFail[mode]; !ok {
		return fmt.Errorf("unknown LEDGER_MODE %q", mode)
	}
	return nil
}

func Verdict(mode string, results []Result) (bool, string) {
	var failed []string
	for _, r := range results {
		if !r.Pass {
			failed = append(failed, r.ID)
		}
	}
	expected := mustFail[mode]
	if len(expected) == 0 {
		if len(failed) == 0 {
			return true, mode + " passes every invariant"
		}
		return false, mode + " fails " + strings.Join(failed, ", ")
	}
	var missing []string
	for _, id := range expected {
		if !slices.Contains(failed, id) {
			missing = append(missing, id)
		}
	}
	if len(missing) > 0 {
		return false, fmt.Sprintf("%s must fail %s but passes %s", mode, strings.Join(expected, ", "), strings.Join(missing, ", "))
	}
	return true, fmt.Sprintf("%s fails %s, and must fail %s", mode, strings.Join(failed, ", "), strings.Join(expected, ", "))
}
