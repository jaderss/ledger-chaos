package main

import (
	"encoding/json"
	"os"
	"sync"
	"time"
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

type FaultEntry struct {
	T     int64  `json:"t"`
	Fault string `json:"fault"`
	Phase string `json:"phase"`
	Error string `json:"error,omitempty"`
}

type Log struct {
	mu   sync.Mutex
	file *os.File
	enc  *json.Encoder
}

func OpenLog(path string) (*Log, error) {
	f, err := os.Create(path)
	if err != nil {
		return nil, err
	}
	return &Log{file: f, enc: json.NewEncoder(f)}, nil
}

func (l *Log) Write(v any) {
	l.mu.Lock()
	defer l.mu.Unlock()
	_ = l.enc.Encode(v)
}

func (l *Log) Close() error {
	return l.file.Close()
}

func now() int64 {
	return time.Now().UnixMilli()
}
