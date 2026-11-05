package main

import (
	"bytes"
	"strings"
	"testing"
)

func TestReadHistoryGroupsAnswersAndMismatchesByKey(t *testing.T) {
	lines := strings.Join([]string{
		`{"op":"transfer","key":"k","phase":"invoke","body":{"from":"a","to":"b","amount":5,"currency":"EUR"}}`,
		`{"op":"transfer","key":"k","phase":"unknown","error":"timeout"}`,
		`{"op":"transfer","key":"k","phase":"invoke","body":{"from":"a","to":"b","amount":5,"currency":"EUR"}}`,
		`{"op":"transfer","key":"k","phase":"ok","status":200,"transferId":"t","transferStatus":"POSTED","replayed":true}`,
		`{"op":"transfer","key":"k","phase":"invoke","mismatch":true,"body":{"from":"a","to":"b","amount":6,"currency":"EUR"}}`,
		`{"op":"transfer","key":"k","phase":"fail","mismatch":true,"status":422,"problem":"idempotency-key-mismatch"}`,
		`{"op":"read","key":"ignored","phase":"ok"}`,
	}, "\n")

	keys, err := ReadHistory(strings.NewReader(lines))
	if err != nil {
		t.Fatal(err)
	}
	k := keys["k"]
	if len(keys) != 1 || k.Body.Amount != 5 || len(k.Answers) != 2 || !k.posted() || len(k.definite()) != 1 {
		t.Fatalf("unexpected key history %+v", k)
	}
	if len(k.Mismatches) != 1 || k.Mismatches[0].Body.Amount != 6 || k.Mismatches[0].Answer.Problem != "idempotency-key-mismatch" {
		t.Fatalf("unexpected mismatch %+v", k.Mismatches)
	}
}

func TestReadHistoryRejectsBrokenLines(t *testing.T) {
	if _, err := ReadHistory(strings.NewReader("{broken")); err == nil {
		t.Fatal("expected an error")
	}
	orphan := `{"op":"transfer","key":"k","phase":"fail","mismatch":true}`
	if _, err := ReadHistory(strings.NewReader(orphan)); err == nil {
		t.Fatal("expected an error for a mismatch result without invoke")
	}
}

func TestParseEventSumsPostingsPerAccount(t *testing.T) {
	e := parseEvent([]byte(`{"eventId":"e","transferId":"t","postings":[{"accountId":"a","amountMinor":-5},{"accountId":"b","amountMinor":5}]}`), "0/1")
	if e.EventID != "e" || e.TransferID != "t" || e.Postings["a"] != -5 || e.Postings["b"] != 5 {
		t.Fatalf("unexpected event %+v", e)
	}
	if bad := parseEvent([]byte("nope"), "2/7"); bad.EventID != "unparseable at 2/7" || bad.TransferID != "" {
		t.Fatalf("unexpected event %+v", bad)
	}
}

func TestPrintMarksFailures(t *testing.T) {
	var out bytes.Buffer
	s := clean()
	s.Deadlocks = 1
	Print(&out, s, Check(s))
	if !strings.Contains(out.String(), "I11  FAIL") || !strings.Contains(out.String(), "1 keys never answered") || !strings.Contains(out.String(), "1 deadlocks") {
		t.Fatalf("unexpected output:\n%s", out.String())
	}
}
