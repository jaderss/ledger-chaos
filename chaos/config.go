package main

import (
	"fmt"
	"os"
	"strconv"
	"time"
)

type Config struct {
	LedgerURL      string
	ToxiproxyURL   string
	DockerSocket   string
	Project        string
	ResultsDir     string
	Scenario       string
	Accounts       int
	OverdraftEvery int
	Workers        int
	Retries        int
	Duration       time.Duration
	ClientTimeout  time.Duration
	DuplicateRate  float64
	MismatchRate   float64
	FaultInterval  time.Duration
	FaultDuration  time.Duration
	Latency        time.Duration
	Funding        int64
	AmountMax      int64
	Seed           uint64
}

type env struct{ errs []error }

func (e *env) str(name, fallback string) string {
	if v := os.Getenv(name); v != "" {
		return v
	}
	return fallback
}

func (e *env) integer(name string, fallback int64) int64 {
	v, err := strconv.ParseInt(e.str(name, strconv.FormatInt(fallback, 10)), 10, 64)
	if err != nil {
		e.errs = append(e.errs, fmt.Errorf("%s: %w", name, err))
	}
	return v
}

func (e *env) float(name string, fallback float64) float64 {
	v, err := strconv.ParseFloat(e.str(name, strconv.FormatFloat(fallback, 'f', -1, 64)), 64)
	if err != nil {
		e.errs = append(e.errs, fmt.Errorf("%s: %w", name, err))
	}
	return v
}

func (e *env) duration(name string, fallback time.Duration) time.Duration {
	v, err := time.ParseDuration(e.str(name, fallback.String()))
	if err != nil {
		e.errs = append(e.errs, fmt.Errorf("%s: %w", name, err))
	}
	return v
}

func LoadConfig() (Config, error) {
	e := &env{}
	c := Config{
		LedgerURL:      e.str("LEDGER_URL", "http://ledger:8080"),
		ToxiproxyURL:   e.str("TOXIPROXY_URL", "http://toxiproxy:8474"),
		DockerSocket:   e.str("DOCKER_SOCKET", "/var/run/docker.sock"),
		Project:        e.str("COMPOSE_PROJECT", "ledger-chaos"),
		ResultsDir:     e.str("RUN_DIR", "/results/local"),
		Scenario:       e.str("SCENARIO", "contention"),
		Accounts:       int(e.integer("ACCOUNTS", 20)),
		OverdraftEvery: int(e.integer("OVERDRAFT_EVERY", 10)),
		Workers:        int(e.integer("WORKERS", 64)),
		Retries:        int(e.integer("RETRIES", 5)),
		Duration:       e.duration("DURATION", 2*time.Minute),
		ClientTimeout:  e.duration("CLIENT_TIMEOUT", time.Second),
		DuplicateRate:  e.float("DUPLICATE_RATE", 0.05),
		MismatchRate:   e.float("MISMATCH_RATE", 0.01),
		FaultInterval:  e.duration("FAULT_INTERVAL", 10*time.Second),
		FaultDuration:  e.duration("FAULT_DURATION", 3*time.Second),
		Latency:        e.duration("DB_LATENCY", 1500*time.Millisecond),
		Funding:        e.integer("FUNDING_MINOR", 100_000),
		AmountMax:      e.integer("AMOUNT_MAX_MINOR", 5_000),
		Seed:           uint64(e.integer("SEED", time.Now().UnixNano())),
	}
	if _, ok := Scenarios[c.Scenario]; !ok {
		e.errs = append(e.errs, fmt.Errorf("SCENARIO %q is not one of %v", c.Scenario, ScenarioNames()))
	}
	if c.Accounts < 2 || c.Workers < 1 || c.AmountMax < 1 {
		e.errs = append(e.errs, fmt.Errorf("ACCOUNTS needs at least 2, WORKERS and AMOUNT_MAX_MINOR at least 1"))
	}
	if len(e.errs) > 0 {
		return c, fmt.Errorf("config: %v", e.errs)
	}
	return c, nil
}
