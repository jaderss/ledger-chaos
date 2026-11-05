package main

import (
	"fmt"
	"maps"
	"slices"
)

type Transfer struct {
	ID     string
	Key    string
	Status string
	From   string
	To     string
	Amount int64
}

type Account struct {
	ID        string
	Overdraft bool
	Balance   int64
}

type OutboxRow struct {
	TransferID string
	Published  bool
}

type Event struct {
	EventID    string
	TransferID string
	Postings   map[string]int64
}

type Snapshot struct {
	Mode              string
	Keys              map[string]*KeyHistory
	Transfers         []Transfer
	Accounts          []Account
	CurrencySums      map[string]int64
	PostingSums       map[string]int64
	TransferPostings  map[string]map[string]int64
	Outbox            []OutboxRow
	Deadlocks         int64
	Events            []Event
	DeadLetters       int64
	ProjectorBalances map[string]int64
	Quiet             bool
	QuietDetail       string
}

type Result struct {
	ID       string   `json:"id"`
	Rule     string   `json:"rule"`
	Pass     bool     `json:"pass"`
	Count    int      `json:"violations"`
	Examples []string `json:"examples,omitempty"`
}

type violations struct {
	count    int
	examples []string
}

func (v *violations) add(format string, args ...any) {
	v.count++
	if len(v.examples) < 5 {
		v.examples = append(v.examples, fmt.Sprintf(format, args...))
	}
}

func (v violations) result(id, rule string) Result {
	return Result{ID: id, Rule: rule, Pass: v.count == 0, Count: v.count, Examples: v.examples}
}

func Check(s Snapshot) []Result {
	byKey := map[string][]Transfer{}
	for _, t := range s.Transfers {
		byKey[t.Key] = append(byKey[t.Key], t)
	}
	return []Result{
		i1(s), i2(s), i3(s), i4(s, byKey), i5(s, byKey), i6(s, byKey), i7(s, byKey), i8(s), i9(s), i11(s),
	}
}

func posted(transfers []Transfer) int {
	n := 0
	for _, t := range transfers {
		if t.Status == "POSTED" {
			n++
		}
	}
	return n
}

func sortedKeys[V any](m map[string]V) []string {
	return slices.Sorted(maps.Keys(m))
}

func i1(s Snapshot) Result {
	var v violations
	for _, currency := range sortedKeys(s.CurrencySums) {
		if sum := s.CurrencySums[currency]; sum != 0 {
			v.add("%s postings sum to %d", currency, sum)
		}
	}
	return v.result("I1", "sum of all postings per currency is zero")
}

func i2(s Snapshot) Result {
	var v violations
	for _, a := range s.Accounts {
		if a.Balance != s.PostingSums[a.ID] {
			v.add("account %s stores %d, postings sum to %d", a.ID, a.Balance, s.PostingSums[a.ID])
		}
	}
	return v.result("I2", "each account balance equals the sum of its postings")
}

func i3(s Snapshot) Result {
	var v violations
	for _, a := range s.Accounts {
		if !a.Overdraft && s.PostingSums[a.ID] < 0 {
			v.add("account %s without overdraft has postings summing to %d", a.ID, s.PostingSums[a.ID])
		}
	}
	return v.result("I3", "no account without overdraft goes below zero")
}

func i4(s Snapshot, byKey map[string][]Transfer) Result {
	var v violations
	for _, key := range sortedKeys(s.Keys) {
		if s.Keys[key].posted() {
			if n := posted(byKey[key]); n != 1 {
				v.add("key %s was acknowledged and has %d posted transfers", key, n)
			}
		}
	}
	return v.result("I4", "an acknowledged transfer exists exactly once")
}

func i5(s Snapshot, byKey map[string][]Transfer) Result {
	var v violations
	for _, key := range sortedKeys(s.Keys) {
		k := s.Keys[key]
		if k.posted() || len(k.definite()) == 0 {
			continue
		}
		if n := posted(byKey[key]); n != 0 {
			v.add("key %s was refused with %d and has %d posted transfers", key, k.definite()[0].Status, n)
		}
	}
	return v.result("I5", "a transfer refused with a definite error does not exist")
}

func i6(s Snapshot, byKey map[string][]Transfer) Result {
	var v violations
	for _, key := range sortedKeys(s.Keys) {
		k := s.Keys[key]
		if len(k.Answers) == 0 || len(k.definite()) > 0 {
			continue
		}
		if n := len(byKey[key]); n > 1 {
			v.add("key %s had only unknown outcomes and has %d transfers", key, n)
		}
	}
	return v.result("I6", "a transfer with unknown outcome exists at most once")
}

func i7(s Snapshot, byKey map[string][]Transfer) Result {
	var v violations
	for _, key := range sortedKeys(s.Keys) {
		k := s.Keys[key]
		var first *Answer
		for _, a := range k.definite() {
			if a.TransferID == "" {
				continue
			}
			if first == nil {
				first = &a
				continue
			}
			if a.TransferID != first.TransferID || a.TransferStatus != first.TransferStatus {
				v.add("key %s answered %s %s, then %s %s", key, first.TransferID, first.TransferStatus,
					a.TransferID, a.TransferStatus)
			}
		}
		for _, m := range k.Mismatches {
			if m.Answer != nil && m.Answer.Phase != "unknown" && m.Answer.Problem != "idempotency-key-mismatch" {
				v.add("key %s reused with another body answered %d %s", key, m.Answer.Status, m.Answer.Problem)
			}
			for _, t := range byKey[key] {
				if t.From == m.Body.From && t.To == m.Body.To && t.Amount == m.Body.Amount {
					v.add("key %s reused with another body created transfer %s", key, t.ID)
				}
			}
		}
	}
	return v.result("I7", "same key and body give the same answer, another body changes nothing")
}

func i8(s Snapshot) Result {
	var v violations
	if !s.Quiet {
		v.add("system never got quiet: %s", s.QuietDetail)
	}
	onTopic := map[string]bool{}
	transfers := map[string]Transfer{}
	for _, t := range s.Transfers {
		transfers[t.ID] = t
	}
	for _, e := range s.Events {
		onTopic[e.TransferID] = true
		t, ok := transfers[e.TransferID]
		if !ok || t.Status != "POSTED" {
			v.add("event %s for transfer %s has no posted transfer", e.EventID, e.TransferID)
			continue
		}
		if !maps.Equal(e.Postings, s.TransferPostings[e.TransferID]) {
			v.add("event %s postings %v differ from the ledger's %v", e.EventID, e.Postings, s.TransferPostings[e.TransferID])
		}
	}
	for _, t := range s.Transfers {
		if t.Status == "POSTED" && !onTopic[t.ID] {
			v.add("posted transfer %s is not on the topic", t.ID)
		}
	}
	if s.Mode == "correct" {
		rows := map[string]int{}
		for _, o := range s.Outbox {
			rows[o.TransferID]++
			if !o.Published {
				v.add("outbox row for transfer %s is unpublished", o.TransferID)
			}
		}
		for _, t := range s.Transfers {
			if t.Status == "POSTED" && rows[t.ID] != 1 {
				v.add("posted transfer %s has %d outbox rows", t.ID, rows[t.ID])
			}
		}
	}
	return v.result("I8", "every posted transfer reaches the topic, and only those")
}

func i9(s Snapshot) Result {
	var v violations
	if !s.Quiet {
		v.add("system never got quiet: %s", s.QuietDetail)
	}
	if s.DeadLetters > 0 {
		v.add("%d records in the DLT", s.DeadLetters)
	}
	accounts := maps.Clone(s.PostingSums)
	for id := range s.ProjectorBalances {
		accounts[id] += 0
	}
	for _, id := range sortedKeys(accounts) {
		if s.ProjectorBalances[id] != s.PostingSums[id] {
			v.add("account %s projected %d, ledger postings sum to %d", id, s.ProjectorBalances[id], s.PostingSums[id])
		}
	}
	return v.result("I9", "projector balances equal the ledger's sums of postings")
}

func i11(s Snapshot) Result {
	var v violations
	if s.Deadlocks > 0 {
		v.add("%d deadlocks in the ledger database", s.Deadlocks)
	}
	return v.result("I11", "no transfer transaction ends in a deadlock")
}
