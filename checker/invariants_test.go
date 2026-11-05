package main

import (
	"strings"
	"testing"
)

const (
	alice = "a1"
	bob   = "b2"
	mint  = "m0"
)

func clean() Snapshot {
	return Snapshot{
		Mode: "correct",
		Keys: map[string]*KeyHistory{
			"fund":     {Key: "fund", Body: Body{From: mint, To: alice, Amount: 100}, Answers: []Answer{{Phase: "ok", Status: 201, TransferID: "t1", TransferStatus: "POSTED"}}},
			"pay":      {Key: "pay", Body: Body{From: alice, To: bob, Amount: 30}, Answers: []Answer{{Phase: "unknown"}, {Phase: "ok", Status: 200, TransferID: "t2", TransferStatus: "POSTED", Replayed: true}}},
			"too-much": {Key: "too-much", Body: Body{From: bob, To: alice, Amount: 999}, Answers: []Answer{{Phase: "fail", Status: 422, TransferID: "t3", TransferStatus: "REJECTED", Problem: "insufficient-funds"}}},
			"lost":     {Key: "lost", Body: Body{From: alice, To: bob, Amount: 5}, Answers: []Answer{{Phase: "unknown"}, {Phase: "unknown"}}},
		},
		Transfers: []Transfer{
			{ID: "t1", Key: "fund", Status: "POSTED", From: mint, To: alice, Amount: 100},
			{ID: "t2", Key: "pay", Status: "POSTED", From: alice, To: bob, Amount: 30},
			{ID: "t3", Key: "too-much", Status: "REJECTED", From: bob, To: alice, Amount: 999},
		},
		Accounts:     []Account{{ID: mint, Overdraft: true, Balance: -100}, {ID: alice, Balance: 70}, {ID: bob, Balance: 30}},
		CurrencySums: map[string]int64{"EUR": 0},
		PostingSums:  map[string]int64{mint: -100, alice: 70, bob: 30},
		TransferPostings: map[string]map[string]int64{
			"t1": {mint: -100, alice: 100},
			"t2": {alice: -30, bob: 30},
		},
		Outbox: []OutboxRow{{TransferID: "t1", Published: true}, {TransferID: "t2", Published: true}},
		Events: []Event{
			{EventID: "e1", TransferID: "t1", Postings: map[string]int64{mint: -100, alice: 100}},
			{EventID: "e2", TransferID: "t2", Postings: map[string]int64{alice: -30, bob: 30}},
			{EventID: "e2", TransferID: "t2", Postings: map[string]int64{alice: -30, bob: 30}},
		},
		ProjectorBalances: map[string]int64{mint: -100, alice: 70, bob: 30},
		Quiet:             true,
	}
}

func failing(results []Result) []string {
	var ids []string
	for _, r := range results {
		if !r.Pass {
			ids = append(ids, r.ID)
		}
	}
	return ids
}

func TestCleanSnapshotPassesEveryInvariant(t *testing.T) {
	if got := failing(Check(clean())); len(got) > 0 {
		t.Fatalf("clean snapshot failed %v", got)
	}
}

func TestEachCorruptionFailsItsInvariant(t *testing.T) {
	cases := []struct {
		name    string
		corrupt func(*Snapshot)
		want    []string
	}{
		{"postings of a currency do not sum to zero", func(s *Snapshot) { s.CurrencySums["EUR"] = 7 }, []string{"I1"}},
		{"stored balance differs from postings", func(s *Snapshot) { s.Accounts[1].Balance = 71 }, []string{"I2"}},
		{"account without overdraft overspent", func(s *Snapshot) {
			s.PostingSums[bob], s.Accounts[2].Balance, s.ProjectorBalances[bob] = -1, -1, -1
		}, []string{"I3"}},
		{"acknowledged key posted twice", func(s *Snapshot) {
			s.Transfers = append(s.Transfers, Transfer{ID: "t9", Key: "pay", Status: "POSTED"})
			s.Outbox = append(s.Outbox, OutboxRow{TransferID: "t9", Published: true})
			s.Events = append(s.Events, Event{EventID: "e9", TransferID: "t9"})
		}, []string{"I4"}},
		{"refused key posted", func(s *Snapshot) {
			s.Transfers[2].Status = "POSTED"
			s.Outbox = append(s.Outbox, OutboxRow{TransferID: "t3", Published: true})
			s.Events = append(s.Events, Event{EventID: "e3", TransferID: "t3"})
		}, []string{"I5"}},
		{"unknown key written twice", func(s *Snapshot) {
			s.Transfers = append(s.Transfers,
				Transfer{ID: "t7", Key: "lost", Status: "REJECTED"}, Transfer{ID: "t8", Key: "lost", Status: "REJECTED"})
		}, []string{"I6"}},
		{"replay answered another transfer", func(s *Snapshot) {
			s.Keys["pay"].Answers = append(s.Keys["pay"].Answers, Answer{Phase: "ok", Status: 200, TransferID: "t5", TransferStatus: "POSTED"})
		}, []string{"I7"}},
		{"mismatch body accepted", func(s *Snapshot) {
			s.Keys["fund"].Mismatches = []MismatchAttempt{{Body: Body{From: mint, To: alice, Amount: 101}, Answer: &Answer{Phase: "ok", Status: 201}}}
		}, []string{"I7"}},
		{"mismatch body created a transfer", func(s *Snapshot) {
			s.Keys["fund"].Mismatches = []MismatchAttempt{{Body: Body{From: mint, To: alice, Amount: 101}}}
			s.Transfers = append(s.Transfers, Transfer{ID: "t6", Key: "fund", Status: "REJECTED", From: mint, To: alice, Amount: 101})
		}, []string{"I7"}},
		{"posted transfer missing from the topic", func(s *Snapshot) { s.Events = s.Events[:1] }, []string{"I8"}},
		{"event with other postings than the ledger", func(s *Snapshot) {
			s.Events[0].Postings = map[string]int64{mint: -101, alice: 101}
		}, []string{"I8"}},
		{"phantom event", func(s *Snapshot) {
			s.Events = append(s.Events, Event{EventID: "x", TransferID: "nope"})
		}, []string{"I8"}},
		{"outbox row left unpublished", func(s *Snapshot) { s.Outbox[0].Published = false }, []string{"I8"}},
		{"projector off by one", func(s *Snapshot) { s.ProjectorBalances[alice] = 69 }, []string{"I9"}},
		{"projector knows an account the ledger does not", func(s *Snapshot) { s.ProjectorBalances["ghost"] = 5 }, []string{"I9"}},
		{"record in the DLT", func(s *Snapshot) { s.DeadLetters = 1 }, []string{"I9"}},
		{"never quiet", func(s *Snapshot) { s.Quiet, s.QuietDetail = false, "lag 3" }, []string{"I8", "I9"}},
		{"deadlock", func(s *Snapshot) { s.Deadlocks = 2 }, []string{"I11"}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s := clean()
			c.corrupt(&s)
			got := failing(Check(s))
			if strings.Join(got, ",") != strings.Join(c.want, ",") {
				t.Fatalf("want %v to fail, got %v", c.want, got)
			}
		})
	}
}

func TestOutboxRowsAreOnlyRequiredInCorrectMode(t *testing.T) {
	s := clean()
	s.Mode = "dual-write"
	s.Outbox = nil
	if got := failing(Check(s)); len(got) > 0 {
		t.Fatalf("dual-write without outbox failed %v", got)
	}
}

func TestExamplesAreCappedButTheCountIsNot(t *testing.T) {
	var v violations
	for i := range 8 {
		v.add("violation %d", i)
	}
	r := v.result("I0", "rule")
	if r.Count != 8 || len(r.Examples) != 5 || r.Pass {
		t.Fatalf("got count %d, %d examples, pass %v", r.Count, len(r.Examples), r.Pass)
	}
}
