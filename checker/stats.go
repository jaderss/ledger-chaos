package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"slices"
)

type Second struct {
	T       int `json:"t"`
	OK      int `json:"ok"`
	Fail    int `json:"fail"`
	Unknown int `json:"unknown"`
}

type FaultWindow struct {
	Fault string  `json:"fault"`
	Start float64 `json:"start"`
	End   float64 `json:"end"`
}

type Stats struct {
	Seconds     float64       `json:"seconds"`
	Keys        int           `json:"keys"`
	Attempts    int           `json:"attempts"`
	OK          int           `json:"ok"`
	Fail        int           `json:"fail"`
	Unknown     int           `json:"unknown"`
	OKPerSecond float64       `json:"okPerSecond"`
	P50Ms       int64         `json:"p50Ms"`
	P99Ms       int64         `json:"p99Ms"`
	MaxMs       int64         `json:"maxMs"`
	Commits     int64         `json:"commits"`
	Rollbacks   int64         `json:"rollbacks"`
	Timeline    []Second      `json:"timeline"`
	Faults      []FaultWindow `json:"faults"`
}

type logEvent struct {
	T        int64  `json:"t"`
	Worker   int    `json:"worker"`
	Key      string `json:"key"`
	Phase    string `json:"phase"`
	Mismatch bool   `json:"mismatch"`
	Fault    string `json:"fault"`
}

func readEvents(r io.Reader) ([]logEvent, error) {
	var out []logEvent
	if r == nil {
		return out, nil
	}
	scanner := bufio.NewScanner(r)
	scanner.Buffer(make([]byte, 64*1024), 1024*1024)
	for scanner.Scan() {
		var e logEvent
		if err := json.Unmarshal(scanner.Bytes(), &e); err != nil {
			return nil, err
		}
		out = append(out, e)
	}
	return out, scanner.Err()
}

func percentile(sorted []int64, p float64) int64 {
	if len(sorted) == 0 {
		return 0
	}
	i := int(p*float64(len(sorted))+0.999999) - 1
	return sorted[max(0, min(i, len(sorted)-1))]
}

func ReadStats(history, faults io.Reader) (Stats, error) {
	var s Stats
	entries, err := readEvents(history)
	if err != nil {
		return s, err
	}
	var start, end int64
	pending := map[string][]int64{}
	var latencies []int64
	buckets := map[int]*Second{}
	keys := map[string]bool{}
	for _, e := range entries {
		if e.Worker < 0 {
			continue
		}
		if start == 0 {
			start = e.T
		}
		end = max(end, e.T)
		id := fmt.Sprintf("%s|%d|%t", e.Key, e.Worker, e.Mismatch)
		if e.Phase == "invoke" {
			if !e.Mismatch {
				keys[e.Key] = true
			}
			pending[id] = append(pending[id], e.T)
			continue
		}
		s.Attempts++
		second := int((e.T - start) / 1000)
		b := buckets[second]
		if b == nil {
			b = &Second{T: second}
			buckets[second] = b
		}
		switch e.Phase {
		case "ok":
			s.OK++
			b.OK++
		case "fail":
			s.Fail++
			b.Fail++
		default:
			s.Unknown++
			b.Unknown++
		}
		if q := pending[id]; len(q) > 0 {
			if e.Phase != "unknown" {
				latencies = append(latencies, e.T-q[0])
			}
			pending[id] = q[1:]
		}
	}
	s.Seconds = float64(end-start) / 1000
	s.Keys = len(keys)
	if s.Seconds > 0 {
		s.OKPerSecond = float64(s.OK) / s.Seconds
	}
	slices.Sort(latencies)
	s.P50Ms, s.P99Ms = percentile(latencies, 0.50), percentile(latencies, 0.99)
	if len(latencies) > 0 {
		s.MaxMs = latencies[len(latencies)-1]
	}
	for t := 0; t <= int(s.Seconds); t++ {
		if b := buckets[t]; b != nil {
			s.Timeline = append(s.Timeline, *b)
		} else {
			s.Timeline = append(s.Timeline, Second{T: t})
		}
	}
	faultEvents, err := readEvents(faults)
	if err != nil {
		return s, err
	}
	open := map[string]float64{}
	for _, f := range faultEvents {
		at := float64(f.T-start) / 1000
		switch f.Phase {
		case "start":
			open[f.Fault] = at
		case "end":
			if from, ok := open[f.Fault]; ok {
				s.Faults = append(s.Faults, FaultWindow{Fault: f.Fault, Start: from, End: at})
				delete(open, f.Fault)
			}
		}
	}
	for fault, from := range open {
		s.Faults = append(s.Faults, FaultWindow{Fault: fault, Start: from, End: s.Seconds})
	}
	return s, nil
}
