package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
)

type Body struct {
	From     string `json:"from"`
	To       string `json:"to"`
	Amount   int64  `json:"amount"`
	Currency string `json:"currency"`
}

type Entry struct {
	T              int64  `json:"t"`
	Worker         int    `json:"worker"`
	Op             string `json:"op"`
	Key            string `json:"key"`
	Phase          string `json:"phase"`
	Body           *Body  `json:"body,omitempty"`
	Mismatch       bool   `json:"mismatch,omitempty"`
	Status         int    `json:"status,omitempty"`
	TransferID     string `json:"transferId,omitempty"`
	TransferStatus string `json:"transferStatus,omitempty"`
	Problem        string `json:"problem,omitempty"`
	Replayed       bool   `json:"replayed,omitempty"`
	Error          string `json:"error,omitempty"`
}

type Answer struct {
	Phase          string
	Status         int
	TransferID     string
	TransferStatus string
	Problem        string
	Replayed       bool
}

type KeyHistory struct {
	Key        string
	Body       Body
	Answers    []Answer
	Mismatches []MismatchAttempt
}

type MismatchAttempt struct {
	Body   Body
	Answer *Answer
}

func (k *KeyHistory) definite() []Answer {
	var out []Answer
	for _, a := range k.Answers {
		if a.Phase != "unknown" {
			out = append(out, a)
		}
	}
	return out
}

func (k *KeyHistory) posted() bool {
	for _, a := range k.Answers {
		if a.Phase == "ok" {
			return true
		}
	}
	return false
}

func ReadHistory(r io.Reader) (map[string]*KeyHistory, error) {
	keys := map[string]*KeyHistory{}
	pendingMismatch := map[string][]int{}
	scanner := bufio.NewScanner(r)
	scanner.Buffer(make([]byte, 64*1024), 1024*1024)
	line := 0
	for scanner.Scan() {
		line++
		var e Entry
		if err := json.Unmarshal(scanner.Bytes(), &e); err != nil {
			return nil, fmt.Errorf("history line %d: %w", line, err)
		}
		if e.Op != "transfer" {
			continue
		}
		k := keys[e.Key]
		if k == nil {
			k = &KeyHistory{Key: e.Key}
			keys[e.Key] = k
		}
		switch {
		case e.Phase == "invoke" && e.Mismatch:
			k.Mismatches = append(k.Mismatches, MismatchAttempt{Body: *e.Body})
			pendingMismatch[e.Key] = append(pendingMismatch[e.Key], len(k.Mismatches)-1)
		case e.Phase == "invoke":
			if k.Body == (Body{}) {
				k.Body = *e.Body
			}
		case e.Mismatch:
			queue := pendingMismatch[e.Key]
			if len(queue) == 0 {
				return nil, fmt.Errorf("history line %d: mismatch result without invoke", line)
			}
			answer := answerOf(e)
			k.Mismatches[queue[0]].Answer = &answer
			pendingMismatch[e.Key] = queue[1:]
		default:
			k.Answers = append(k.Answers, answerOf(e))
		}
	}
	return keys, scanner.Err()
}

func answerOf(e Entry) Answer {
	return Answer{
		Phase: e.Phase, Status: e.Status, TransferID: e.TransferID, TransferStatus: e.TransferStatus,
		Problem: e.Problem, Replayed: e.Replayed,
	}
}
