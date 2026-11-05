package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

func TestClassify(t *testing.T) {
	cases := []struct {
		name     string
		status   int
		replayed bool
		body     string
		want     Result
	}{
		{"created", 201, false, `{"id":"t","status":"POSTED"}`, Result{Phase: PhaseOK, Status: 201, TransferID: "t", TransferStatus: "POSTED"}},
		{"replayed", 200, true, `{"id":"t","status":"POSTED"}`, Result{Phase: PhaseOK, Status: 200, TransferID: "t", TransferStatus: "POSTED", Replayed: true}},
		{"rejected", 422, false, `{"type":"/problems/insufficient-funds","transferId":"t","transferStatus":"REJECTED"}`,
			Result{Phase: PhaseFail, Status: 422, TransferID: "t", TransferStatus: "REJECTED", Problem: "insufficient-funds"}},
		{"not found", 404, false, `{"type":"/problems/account-not-found"}`, Result{Phase: PhaseFail, Status: 404, Problem: "account-not-found"}},
		{"unavailable", 503, false, `{"type":"/problems/unavailable"}`, Result{Phase: PhaseUnknown, Status: 503, Problem: "unavailable"}},
		{"not json", 502, false, `<html>`, Result{Phase: PhaseUnknown, Status: 502}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := classify(c.status, c.replayed, []byte(c.body)); got != c.want {
				t.Fatalf("got %+v, want %+v", got, c.want)
			}
		})
	}
}

func fakeLedger(t *testing.T, handler http.HandlerFunc) (*Workload, *Log) {
	t.Helper()
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)
	log, err := OpenLog(t.TempDir() + "/history.jsonl")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { log.Close() })
	return &Workload{
		ledger: NewLedgerClient(server.URL), log: log, currency: "EUR", retries: 3,
		clientTimeout: time.Second, amountMax: 10, workers: 2,
	}, log
}

func TestSendRetriesUnknownWithTheSameKeyUntilADefiniteAnswer(t *testing.T) {
	var calls atomic.Int32
	keys := make(chan string, 10)
	w, _ := fakeLedger(t, func(rw http.ResponseWriter, r *http.Request) {
		keys <- r.Header.Get("Idempotency-Key")
		if calls.Add(1) < 3 {
			rw.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		rw.WriteHeader(http.StatusCreated)
		_, _ = rw.Write([]byte(`{"id":"t","status":"POSTED"}`))
	})

	w.send(0, "k", Body{From: "a", To: "b", Amount: 1, Currency: "EUR"})

	close(keys)
	for key := range keys {
		if key != "k" {
			t.Fatalf("retry used key %q", key)
		}
	}
	if calls.Load() != 3 || len(w.settled) != 1 {
		t.Fatalf("calls %d, settled %d", calls.Load(), len(w.settled))
	}
}

func TestSendGivesUpAfterTheRetryBudget(t *testing.T) {
	var calls atomic.Int32
	w, _ := fakeLedger(t, func(rw http.ResponseWriter, _ *http.Request) {
		calls.Add(1)
		rw.WriteHeader(http.StatusServiceUnavailable)
	})

	w.send(0, "k", Body{From: "a", To: "b", Amount: 1, Currency: "EUR"})

	if calls.Load() != 4 || len(w.settled) != 0 {
		t.Fatalf("calls %d, settled %d", calls.Load(), len(w.settled))
	}
}

func TestRefusedTransfersAreNotReusedForMismatches(t *testing.T) {
	w, _ := fakeLedger(t, func(rw http.ResponseWriter, _ *http.Request) {
		rw.WriteHeader(http.StatusUnprocessableEntity)
	})

	w.send(0, "k", Body{From: "a", To: "b", Amount: 1, Currency: "EUR"})

	if len(w.settled) != 0 {
		t.Fatal("a refused key must not be offered for mismatches")
	}
}

func TestSetupFundAndRun(t *testing.T) {
	var transfers atomic.Int32
	w, _ := fakeLedger(t, func(rw http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/accounts":
			rw.WriteHeader(http.StatusCreated)
		default:
			transfers.Add(1)
			rw.WriteHeader(http.StatusCreated)
			_, _ = rw.Write([]byte(`{"id":"t","status":"POSTED"}`))
		}
	})
	w.duplicateRate, w.mismatchRate = 0.3, 0.3

	if err := w.Setup(context.Background(), 4, 2); err != nil {
		t.Fatal(err)
	}
	if err := w.Fund(context.Background(), 100); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	w.Run(ctx)

	if len(w.accounts) != 4 || transfers.Load() <= 4 {
		t.Fatalf("accounts %d, transfers %d", len(w.accounts), transfers.Load())
	}
}

func TestFundStopsOnARefusal(t *testing.T) {
	w, _ := fakeLedger(t, func(rw http.ResponseWriter, _ *http.Request) {
		rw.WriteHeader(http.StatusNotFound)
	})
	w.accounts = []string{"a"}
	if err := w.Fund(context.Background(), 100); err == nil {
		t.Fatal("expected funding to fail")
	}
}

func TestCreateAccountFailsOnConflict(t *testing.T) {
	w, _ := fakeLedger(t, func(rw http.ResponseWriter, _ *http.Request) {
		rw.WriteHeader(http.StatusConflict)
	})
	if err := w.ledger.CreateAccount(context.Background(), "a", "EUR", false); err == nil {
		t.Fatal("expected an error")
	}
}
