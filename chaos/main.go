package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"math/rand/v2"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"
)

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func run() error {
	cfg, err := LoadConfig()
	if err != nil {
		return err
	}
	if err := os.MkdirAll(cfg.ResultsDir, 0o755); err != nil {
		return err
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	history, err := OpenLog(filepath.Join(cfg.ResultsDir, "history.jsonl"))
	if err != nil {
		return err
	}
	defer history.Close()
	faults, err := OpenLog(filepath.Join(cfg.ResultsDir, "faults.jsonl"))
	if err != nil {
		return err
	}
	defer faults.Close()

	catalog := Catalog(NewDocker(cfg.DockerSocket, cfg.Project), NewToxiproxy(cfg.ToxiproxyURL), cfg.Latency)
	if err := HealAll(ctx, catalog); err != nil {
		log.Printf("heal before start: %v", err)
	}

	workload := &Workload{
		ledger: NewLedgerClient(cfg.LedgerURL, 2*cfg.Workers), log: history, currency: "EUR", workers: cfg.Workers,
		retries: cfg.Retries, clientTimeout: cfg.ClientTimeout, amountMax: cfg.AmountMax,
		duplicateRate: cfg.DuplicateRate, mismatchRate: cfg.MismatchRate, seed: cfg.Seed,
	}
	log.Printf("seed %d, scenario %s, %d workers for %s", cfg.Seed, cfg.Scenario, cfg.Workers, cfg.Duration)
	if err := workload.Setup(ctx, cfg.Accounts, cfg.OverdraftEvery); err != nil {
		return fmt.Errorf("setup: %w", err)
	}
	if err := workload.Fund(ctx, cfg.Funding); err != nil {
		return fmt.Errorf("funding: %w", err)
	}

	runCtx, cancel := context.WithTimeout(ctx, cfg.Duration)
	defer cancel()
	nemesis := &Nemesis{
		catalog: catalog, faults: Scenarios[cfg.Scenario], interval: cfg.FaultInterval,
		duration: cfg.FaultDuration, rng: rand.New(rand.NewPCG(cfg.Seed, 0)), log: faults,
	}
	started := time.Now()
	done := make(chan struct{})
	go func() {
		nemesis.Run(runCtx)
		close(done)
	}()
	workload.Run(runCtx)
	<-done

	healCtx, cancelHeal := context.WithTimeout(context.Background(), time.Minute)
	defer cancelHeal()
	if err := HealAll(healCtx, catalog); err != nil {
		log.Printf("heal after run: %v", err)
	}
	return writeRun(cfg, started)
}

func writeRun(cfg Config, started time.Time) error {
	f, err := os.Create(filepath.Join(cfg.ResultsDir, "run.json"))
	if err != nil {
		return err
	}
	defer f.Close()
	return json.NewEncoder(f).Encode(map[string]any{
		"seed": cfg.Seed, "scenario": cfg.Scenario, "faults": Scenarios[cfg.Scenario],
		"accounts": cfg.Accounts, "workers": cfg.Workers, "duration": cfg.Duration.String(),
		"clientTimeout": cfg.ClientTimeout.String(), "fundingMinor": cfg.Funding, "amountMaxMinor": cfg.AmountMax,
		"startedAt": started.UTC().Format(time.RFC3339), "endedAt": time.Now().UTC().Format(time.RFC3339),
	})
}
