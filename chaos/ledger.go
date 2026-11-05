package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"path"
)

const (
	PhaseOK      = "ok"
	PhaseFail    = "fail"
	PhaseUnknown = "unknown"
)

type Result struct {
	Phase          string
	Status         int
	TransferID     string
	TransferStatus string
	Problem        string
	Replayed       bool
	Error          string
}

type LedgerClient struct {
	base string
	http *http.Client
}

func NewLedgerClient(base string, conns int) *LedgerClient {
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.MaxIdleConns = conns
	transport.MaxIdleConnsPerHost = conns
	return &LedgerClient{base: base, http: &http.Client{Transport: transport}}
}

func (l *LedgerClient) CreateAccount(ctx context.Context, id, currency string, overdraft bool) error {
	body, _ := json.Marshal(map[string]any{"id": id, "currency": currency, "allowOverdraft": overdraft})
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, l.base+"/accounts", bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	res, err := l.http.Do(req)
	if err != nil {
		return err
	}
	defer res.Body.Close()
	_, _ = io.Copy(io.Discard, res.Body)
	if res.StatusCode != http.StatusCreated && res.StatusCode != http.StatusOK {
		return fmt.Errorf("create account %s: status %d", id, res.StatusCode)
	}
	return nil
}

func (l *LedgerClient) Transfer(ctx context.Context, key string, b Body) Result {
	payload, _ := json.Marshal(map[string]any{
		"fromAccount": b.From, "toAccount": b.To, "amountMinor": b.Amount, "currency": b.Currency,
	})
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, l.base+"/transfers", bytes.NewReader(payload))
	if err != nil {
		return Result{Phase: PhaseUnknown, Error: err.Error()}
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Idempotency-Key", key)
	req.Header.Set("X-Request-Id", key)
	res, err := l.http.Do(req)
	if err != nil {
		return Result{Phase: PhaseUnknown, Error: err.Error()}
	}
	defer res.Body.Close()
	data, err := io.ReadAll(res.Body)
	if err != nil {
		return Result{Phase: PhaseUnknown, Status: res.StatusCode, Error: err.Error()}
	}
	return classify(res.StatusCode, res.Header.Get("Idempotent-Replayed") == "true", data)
}

func classify(status int, replayed bool, data []byte) Result {
	var body struct {
		ID             string `json:"id"`
		Status         string `json:"status"`
		Type           string `json:"type"`
		TransferID     string `json:"transferId"`
		TransferStatus string `json:"transferStatus"`
	}
	_ = json.Unmarshal(data, &body)
	r := Result{Status: status, Replayed: replayed}
	switch {
	case status == http.StatusCreated || status == http.StatusOK:
		r.Phase, r.TransferID, r.TransferStatus = PhaseOK, body.ID, body.Status
	case status >= 400 && status < 500:
		r.Phase, r.TransferID, r.TransferStatus = PhaseFail, body.TransferID, body.TransferStatus
		if body.Type != "" {
			r.Problem = path.Base(body.Type)
		}
	default:
		r.Phase = PhaseUnknown
		if body.Type != "" {
			r.Problem = path.Base(body.Type)
		}
	}
	return r
}
