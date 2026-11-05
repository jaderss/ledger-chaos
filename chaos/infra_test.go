package main

import (
	"context"
	"encoding/json"
	"io"
	"math/rand/v2"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

type recorder struct {
	mu    sync.Mutex
	calls []string
}

func (r *recorder) add(call string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.calls = append(r.calls, call)
}

func (r *recorder) all() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.calls...)
}

func fakeDocker(t *testing.T) (*Docker, *recorder) {
	t.Helper()
	socket := filepath.Join(t.TempDir(), "docker.sock")
	listener, err := net.Listen("unix", socket)
	if err != nil {
		t.Fatal(err)
	}
	rec := &recorder{}
	server := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet {
			if strings.Contains(r.URL.Query().Get("filters"), "service=missing") {
				_, _ = w.Write([]byte(`[]`))
				return
			}
			_, _ = w.Write([]byte(`[{"Id":"c1"}]`))
			return
		}
		rec.add(r.URL.Path + "?" + r.URL.RawQuery)
		if strings.HasSuffix(r.URL.Path, "/unpause") {
			w.WriteHeader(http.StatusInternalServerError)
			_, _ = w.Write([]byte(`{"message":"Container c1 is not paused"}`))
			return
		}
		if strings.HasSuffix(r.URL.Path, "/pause") {
			w.WriteHeader(http.StatusConflict)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	})}
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(func() { _ = server.Close() })
	return NewDocker(socket, "ledger-chaos"), rec
}

func TestDockerActionsHitTheComposeContainer(t *testing.T) {
	d, rec := fakeDocker(t)
	ctx := context.Background()
	for _, err := range []error{d.Kill(ctx, "ledger"), d.Start(ctx, "ledger"), d.Stop(ctx, "kafka"), d.Restart(ctx, "postgres"), d.Unpause(ctx, "postgres")} {
		if err != nil {
			t.Fatal(err)
		}
	}
	want := []string{"/containers/c1/kill?signal=SIGKILL", "/containers/c1/start?", "/containers/c1/stop?t=0",
		"/containers/c1/restart?t=0", "/containers/c1/unpause?"}
	if got := rec.all(); strings.Join(got, " ") != strings.Join(want, " ") {
		t.Fatalf("got %v", got)
	}
	if err := d.Pause(ctx, "postgres"); err == nil {
		t.Fatal("expected a conflict to be an error")
	}
	if err := d.Kill(ctx, "missing"); err == nil {
		t.Fatal("expected an error for a missing service")
	}
}

func TestToxiproxyCalls(t *testing.T) {
	rec := &recorder{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		rec.add(r.Method + " " + r.URL.Path + " " + string(body))
		if r.Method == http.MethodDelete {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		if strings.Contains(r.URL.Path, "broken") {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()
	tp := NewToxiproxy(server.URL)
	ctx := context.Background()

	if err := tp.SetEnabled(ctx, "postgres", false); err != nil {
		t.Fatal(err)
	}
	if err := tp.AddToxic(ctx, "postgres", "latency", "latency", map[string]int{"latency": 1500}); err != nil {
		t.Fatal(err)
	}
	if err := tp.RemoveToxic(ctx, "postgres", "latency"); err != nil {
		t.Fatal("removing a missing toxic must count as healed")
	}
	if err := tp.SetEnabled(ctx, "broken", true); err == nil {
		t.Fatal("expected an error")
	}
	calls := rec.all()
	var toxic map[string]any
	_ = json.Unmarshal([]byte(strings.SplitN(calls[1], " ", 3)[2]), &toxic)
	if calls[0] != `POST /proxies/postgres {"enabled":false}` || toxic["type"] != "latency" || toxic["stream"] != "downstream" {
		t.Fatalf("unexpected calls %v", calls)
	}
}

func TestNemesisAppliesAndHealsEachFaultAndLogsIt(t *testing.T) {
	rec := &recorder{}
	catalog := map[string]Fault{"f": {
		Apply: func(context.Context) error { rec.add("apply"); return nil },
		Heal:  func(context.Context) error { rec.add("heal"); return nil },
	}}
	path := filepath.Join(t.TempDir(), "faults.jsonl")
	log, _ := OpenLog(path)
	n := &Nemesis{catalog: catalog, faults: []string{"f"}, interval: 20 * time.Millisecond,
		duration: 10 * time.Millisecond, rng: rand.New(rand.NewPCG(1, 2)), log: log}

	ctx, cancel := context.WithTimeout(context.Background(), 70*time.Millisecond)
	defer cancel()
	n.Run(ctx)
	log.Close()

	calls := rec.all()
	if len(calls) < 2 || len(calls)%2 != 0 || calls[0] != "apply" || calls[len(calls)-1] != "heal" {
		t.Fatalf("unexpected calls %v", calls)
	}
	data, _ := os.ReadFile(path)
	if strings.Count(string(data), `"phase":"start"`) != strings.Count(string(data), `"phase":"end"`) {
		t.Fatalf("unbalanced fault log:\n%s", data)
	}
}

func TestEveryScenarioFaultIsInTheCatalog(t *testing.T) {
	catalog := Catalog(nil, nil, time.Second)
	for _, name := range ScenarioNames() {
		for _, fault := range Scenarios[name] {
			if _, ok := catalog[fault]; !ok {
				t.Fatalf("scenario %s uses unknown fault %s", name, fault)
			}
		}
	}
}

func TestHealAllCallsEveryHealAndJoinsErrors(t *testing.T) {
	d, rec := fakeDocker(t)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer server.Close()

	err := HealAll(context.Background(), Catalog(d, NewToxiproxy(server.URL), time.Second))

	if err == nil || !strings.Contains(err.Error(), "db-cut") {
		t.Fatalf("expected the toxiproxy heal errors, got %v", err)
	}
	if len(rec.all()) == 0 {
		t.Fatal("expected docker heals")
	}
}

func TestLoadConfigRejectsAnUnknownScenario(t *testing.T) {
	t.Setenv("SCENARIO", "nope")
	if _, err := LoadConfig(); err == nil {
		t.Fatal("expected an error")
	}
}

func TestLoadConfigReadsTheEnvironment(t *testing.T) {
	t.Setenv("SCENARIO", "kills")
	t.Setenv("WORKERS", "3")
	t.Setenv("DURATION", "5s")
	t.Setenv("DUPLICATE_RATE", "0.5")
	t.Setenv("SEED", "42")
	c, err := LoadConfig()
	if err != nil || c.Workers != 3 || c.Duration != 5*time.Second || c.DuplicateRate != 0.5 || c.Seed != 42 {
		t.Fatalf("config %+v, err %v", c, err)
	}
	t.Setenv("WORKERS", "many")
	if _, err := LoadConfig(); err == nil {
		t.Fatal("expected an error for a bad number")
	}
}

func TestRandomBodyNeverPicksTheSameAccountTwice(t *testing.T) {
	w := &Workload{accounts: []string{"a", "b", "c"}, amountMax: 5, currency: "EUR"}
	rng := rand.New(rand.NewPCG(1, 1))
	for range 1000 {
		b := w.randomBody(rng)
		if b.From == b.To || b.Amount < 1 || b.Amount > 5 {
			t.Fatalf("bad body %+v", b)
		}
	}
}
