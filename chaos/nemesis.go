package main

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"math/rand/v2"
	"slices"
	"sort"
	"time"
)

type Fault struct {
	Apply func(context.Context) error
	Heal  func(context.Context) error
}

var Scenarios = map[string][]string{
	"contention": {},
	"db-latency": {"db-latency"},
	"kafka-down": {"kafka-down"},
	"kills":      {"kill-ledger", "kill-relay", "kill-projector"},
	"db-faults":  {"pause-db", "restart-db", "db-cut", "db-reset"},
	"all": {"kill-ledger", "kill-relay", "kill-projector", "pause-db", "restart-db", "db-cut", "db-latency",
		"db-reset", "kafka-down"},
}

func ScenarioNames() []string {
	names := make([]string, 0, len(Scenarios))
	for name := range Scenarios {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

func Catalog(d *Docker, t *Toxiproxy, latency time.Duration) map[string]Fault {
	kill := func(service string) Fault {
		return Fault{
			Apply: func(ctx context.Context) error { return d.Kill(ctx, service) },
			Heal:  func(ctx context.Context) error { return d.Start(ctx, service) },
		}
	}
	noop := func(context.Context) error { return nil }
	return map[string]Fault{
		"kill-ledger":    kill("ledger"),
		"kill-relay":     kill("relay"),
		"kill-projector": kill("projector"),
		"pause-db": {
			Apply: func(ctx context.Context) error { return d.Pause(ctx, "postgres") },
			Heal:  func(ctx context.Context) error { return d.Unpause(ctx, "postgres") },
		},
		"restart-db": {
			Apply: func(ctx context.Context) error { return d.Restart(ctx, "postgres") },
			Heal:  noop,
		},
		"db-cut": {
			Apply: func(ctx context.Context) error { return t.SetEnabled(ctx, "postgres", false) },
			Heal:  func(ctx context.Context) error { return t.SetEnabled(ctx, "postgres", true) },
		},
		"db-latency": {
			Apply: func(ctx context.Context) error {
				return t.AddToxic(ctx, "postgres", "latency", "latency", map[string]int{"latency": int(latency.Milliseconds())})
			},
			Heal: func(ctx context.Context) error { return t.RemoveToxic(ctx, "postgres", "latency") },
		},
		"db-reset": {
			Apply: func(ctx context.Context) error {
				return t.AddToxic(ctx, "postgres", "reset", "reset_peer", map[string]int{"timeout": 0})
			},
			Heal: func(ctx context.Context) error { return t.RemoveToxic(ctx, "postgres", "reset") },
		},
		"kafka-down": {
			Apply: func(ctx context.Context) error { return d.Stop(ctx, "kafka") },
			Heal:  func(ctx context.Context) error { return d.Start(ctx, "kafka") },
		},
	}
}

func HealAll(ctx context.Context, catalog map[string]Fault) error {
	var errs []error
	for _, name := range slices.Sorted(maps.Keys(catalog)) {
		if err := catalog[name].Heal(ctx); err != nil {
			errs = append(errs, fmt.Errorf("%s: %w", name, err))
		}
	}
	return errors.Join(errs...)
}

type Nemesis struct {
	catalog  map[string]Fault
	faults   []string
	interval time.Duration
	duration time.Duration
	rng      *rand.Rand
	log      *Log
}

func (n *Nemesis) Run(ctx context.Context) {
	if len(n.faults) == 0 {
		<-ctx.Done()
		return
	}
	ticker := time.NewTicker(n.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			n.inject(ctx, n.faults[n.rng.IntN(len(n.faults))])
		}
	}
}

func (n *Nemesis) inject(ctx context.Context, name string) {
	fault := n.catalog[name]
	n.log.Write(FaultEntry{T: now(), Fault: name, Phase: "start"})
	if err := fault.Apply(context.WithoutCancel(ctx)); err != nil {
		n.log.Write(FaultEntry{T: now(), Fault: name, Phase: "error", Error: err.Error()})
	}
	select {
	case <-ctx.Done():
	case <-time.After(n.duration):
	}
	healCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
	defer cancel()
	if err := fault.Heal(healCtx); err != nil {
		n.log.Write(FaultEntry{T: now(), Fault: name, Phase: "error", Error: err.Error()})
	}
	n.log.Write(FaultEntry{T: now(), Fault: name, Phase: "end"})
}
