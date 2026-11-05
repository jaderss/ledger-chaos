package main

import (
	"strings"
	"testing"
)

const statsHistory = `{"t":500,"worker":-1,"key":"fund","phase":"invoke"}
{"t":600,"worker":-1,"key":"fund","phase":"ok"}
{"t":1000,"worker":0,"key":"a","phase":"invoke"}
{"t":1100,"worker":0,"key":"a","phase":"ok"}
{"t":1200,"worker":1,"key":"b","phase":"invoke"}
{"t":1500,"worker":2,"key":"a","phase":"invoke","mismatch":true}
{"t":1510,"worker":2,"key":"a","phase":"fail","mismatch":true}
{"t":2200,"worker":1,"key":"b","phase":"unknown"}
{"t":2300,"worker":1,"key":"b","phase":"invoke"}
{"t":3500,"worker":1,"key":"b","phase":"ok"}
`

const statsFaults = `{"t":1500,"fault":"pause-db","phase":"start"}
{"t":2000,"fault":"pause-db","phase":"end"}
{"t":3000,"fault":"db-cut","phase":"start"}
`

func TestReadStats(t *testing.T) {
	s, err := ReadStats(strings.NewReader(statsHistory), strings.NewReader(statsFaults))
	if err != nil {
		t.Fatal(err)
	}
	if s.Seconds != 2.5 || s.Keys != 2 || s.Attempts != 4 || s.OK != 2 || s.Fail != 1 || s.Unknown != 1 || s.OKPerSecond != 0.8 {
		t.Fatalf("unexpected counts %+v", s)
	}
	if s.P50Ms != 100 || s.P99Ms != 1200 || s.MaxMs != 1200 {
		t.Fatalf("unexpected latencies p50 %d p99 %d max %d", s.P50Ms, s.P99Ms, s.MaxMs)
	}
	want := []Second{{T: 0, OK: 1, Fail: 1}, {T: 1, Unknown: 1}, {T: 2, OK: 1}}
	if len(s.Timeline) != len(want) {
		t.Fatalf("unexpected timeline %+v", s.Timeline)
	}
	for i, w := range want {
		if s.Timeline[i] != w {
			t.Fatalf("second %d: got %+v, want %+v", i, s.Timeline[i], w)
		}
	}
	if len(s.Faults) != 2 || s.Faults[0] != (FaultWindow{Fault: "pause-db", Start: 0.5, End: 1}) ||
		s.Faults[1] != (FaultWindow{Fault: "db-cut", Start: 2, End: 2.5}) {
		t.Fatalf("unexpected faults %+v", s.Faults)
	}
}

func TestReadStatsWithoutFaultsOrHistory(t *testing.T) {
	s, err := ReadStats(strings.NewReader(""), nil)
	if err != nil || s.Attempts != 0 || s.P99Ms != 0 || len(s.Faults) != 0 || len(s.Timeline) != 1 {
		t.Fatalf("unexpected %+v, %v", s, err)
	}
	if _, err := ReadStats(strings.NewReader("not json\n"), nil); err == nil {
		t.Fatal("expected a parse error")
	}
}
