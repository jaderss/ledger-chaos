package main

import (
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestRunWritesHistoryFaultsAndRunAgainstFakes(t *testing.T) {
	ledger := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/accounts" {
			w.WriteHeader(http.StatusCreated)
			return
		}
		w.WriteHeader(http.StatusCreated)
		_, _ = w.Write([]byte(`{"id":"t","status":"POSTED"}`))
	}))
	defer ledger.Close()
	toxiproxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodDelete {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer toxiproxy.Close()
	docker, rec := fakeDocker(t)
	dir := t.TempDir()

	t.Setenv("LEDGER_URL", ledger.URL)
	t.Setenv("TOXIPROXY_URL", toxiproxy.URL)
	t.Setenv("DOCKER_SOCKET", docker.socket)
	t.Setenv("RUN_DIR", dir)
	t.Setenv("SCENARIO", "kills")
	t.Setenv("ACCOUNTS", "3")
	t.Setenv("WORKERS", "2")
	t.Setenv("DURATION", "300ms")
	t.Setenv("FAULT_INTERVAL", "50ms")
	t.Setenv("FAULT_DURATION", "10ms")
	t.Setenv("SEED", "7")

	if err := run(); err != nil {
		t.Fatal(err)
	}

	history, _ := os.ReadFile(filepath.Join(dir, "history.jsonl"))
	faults, _ := os.ReadFile(filepath.Join(dir, "faults.jsonl"))
	runJSON, _ := os.ReadFile(filepath.Join(dir, "run.json"))
	if !strings.Contains(string(history), `"phase":"ok"`) || !strings.Contains(string(faults), `"phase":"start"`) ||
		!strings.Contains(string(runJSON), `"seed":7`) {
		t.Fatalf("history %d bytes, faults:\n%s\nrun: %s", len(history), faults, runJSON)
	}
	if !strings.Contains(strings.Join(rec.all(), " "), "kill?signal=SIGKILL") {
		t.Fatalf("no kill reached docker: %v", rec.all())
	}
}

func TestRunFailsOnABadConfig(t *testing.T) {
	t.Setenv("SCENARIO", "nope")
	if err := run(); err == nil {
		t.Fatal("expected an error")
	}
}
