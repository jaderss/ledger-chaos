package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
)

func main() {
	ok, err := run(os.Stdout)
	if err != nil {
		log.Fatal(err)
	}
	if !ok {
		os.Exit(1)
	}
}

func getenv(name, fallback string) string {
	if v := os.Getenv(name); v != "" {
		return v
	}
	return fallback
}

func run(out io.Writer) (bool, error) {
	runDir := getenv("RUN_DIR", "/results/local")
	quietTimeout, err := time.ParseDuration(getenv("QUIET_TIMEOUT", "60s"))
	if err != nil {
		return false, fmt.Errorf("QUIET_TIMEOUT: %w", err)
	}
	topic, dlt, group := getenv("TOPIC", "ledger.transfers"), getenv("DLT_TOPIC", "ledger.transfers.DLT"), getenv("GROUP", "projector")
	ctx := context.Background()
	s := Snapshot{Mode: getenv("LEDGER_MODE", "correct")}
	locking := getenv("LEDGER_LOCKING", "ordered")
	if err := knownMode(s.Mode); err != nil {
		return false, err
	}

	history, err := os.Open(filepath.Join(runDir, "history.jsonl"))
	if err != nil {
		return false, err
	}
	defer history.Close()
	if s.Keys, err = ReadHistory(history); err != nil {
		return false, err
	}

	ledger, err := pgx.Connect(ctx, getenv("LEDGER_DSN", "postgres://postgres:postgres@postgres:5432/ledger"))
	if err != nil {
		return false, err
	}
	defer ledger.Close(ctx)
	projector, err := pgx.Connect(ctx, getenv("PROJECTOR_DSN", "postgres://postgres:postgres@postgres:5432/projector"))
	if err != nil {
		return false, err
	}
	defer projector.Close(ctx)
	kafka, err := NewKafka(strings.Split(getenv("KAFKA_BROKERS", "kafka:29092"), ","))
	if err != nil {
		return false, err
	}
	defer kafka.Close()

	s.Quiet, s.QuietDetail = waitQuiet(ctx, ledger, kafka, group, topic, quietTimeout)
	if err := loadLedger(ctx, ledger, &s); err != nil {
		return false, fmt.Errorf("ledger: %w", err)
	}
	if err := loadProjector(ctx, projector, &s); err != nil {
		return false, fmt.Errorf("projector: %w", err)
	}
	if s.Events, err = kafka.Events(ctx, topic); err != nil {
		return false, err
	}
	if s.DeadLetters, err = kafka.Count(ctx, dlt); err != nil {
		return false, err
	}

	stats, err := runStats(runDir)
	if err != nil {
		return false, fmt.Errorf("stats: %w", err)
	}
	stats.Commits, stats.Rollbacks = s.Commits, s.Rollbacks

	results := Check(s)
	Print(out, s, results)
	fmt.Fprintf(out, "%.0f ok/s over %.0f s, p50 %d ms, p99 %d ms, %d commits, %d rollbacks, %d faults\n",
		stats.OKPerSecond, stats.Seconds, stats.P50Ms, stats.P99Ms, stats.Commits, stats.Rollbacks, len(stats.Faults))
	ok, verdict := Verdict(s.Mode, results)
	fmt.Fprintln(out, "verdict:", verdict)
	return ok, writeReport(runDir, report{
		Mode: s.Mode, Locking: locking, MustFail: mustFail[s.Mode], OK: ok, Verdict: verdict,
		Quiet: s.Quiet, QuietDetail: s.QuietDetail, Results: results, Stats: stats,
	})
}

func waitQuiet(ctx context.Context, ledger *pgx.Conn, kafka *Kafka, group, topic string, timeout time.Duration) (bool, string) {
	deadline := time.Now().Add(timeout)
	var detail string
	for {
		rows, err := unpublished(ctx, ledger)
		lag, lagErr := kafka.Lag(ctx, group, topic)
		detail = fmt.Sprintf("%d unpublished outbox rows, projector lag %d", rows, lag)
		if err == nil && lagErr == nil && rows == 0 && lag == 0 {
			return true, detail
		}
		if err != nil || lagErr != nil {
			detail = fmt.Sprintf("%s (errors: %v %v)", detail, err, lagErr)
		}
		if time.Now().After(deadline) {
			return false, detail + " after " + timeout.String()
		}
		time.Sleep(time.Second)
	}
}

func Print(out io.Writer, s Snapshot, results []Result) {
	counts := map[string]int{}
	for _, k := range s.Keys {
		for _, a := range k.Answers {
			counts[a.Phase]++
		}
		counts["mismatch"] += len(k.Mismatches)
		if len(k.Answers) > 0 && len(k.definite()) == 0 {
			counts["unanswered"]++
		}
	}
	fmt.Fprintf(out, "mode %s: %d keys, %d ok, %d fail, %d unknown, %d keys never answered, %d mismatch attempts, %d transfers, %d events\n",
		s.Mode, len(s.Keys), counts["ok"], counts["fail"], counts["unknown"], counts["unanswered"], counts["mismatch"],
		len(s.Transfers), len(s.Events))
	for _, r := range results {
		verdict := "PASS"
		if !r.Pass {
			verdict = "FAIL"
		}
		fmt.Fprintf(out, "%-4s %s %6d  %s\n", r.ID, verdict, r.Count, r.Rule)
		for _, e := range r.Examples {
			fmt.Fprintf(out, "            %s\n", e)
		}
	}
}

type report struct {
	Mode        string         `json:"mode"`
	Locking     string         `json:"locking"`
	MustFail    []string       `json:"mustFail"`
	OK          bool           `json:"ok"`
	Verdict     string         `json:"verdict"`
	Quiet       bool           `json:"quiet"`
	QuietDetail string         `json:"quietDetail"`
	Results     []Result       `json:"results"`
	Stats       Stats          `json:"stats"`
	Run         map[string]any `json:"run,omitempty"`
}

func runStats(runDir string) (Stats, error) {
	history, err := os.Open(filepath.Join(runDir, "history.jsonl"))
	if err != nil {
		return Stats{}, err
	}
	defer history.Close()
	faults, err := os.Open(filepath.Join(runDir, "faults.jsonl"))
	if err != nil {
		return ReadStats(history, nil)
	}
	defer faults.Close()
	return ReadStats(history, faults)
}

func writeReport(runDir string, report report) error {
	if data, err := os.ReadFile(filepath.Join(runDir, "run.json")); err == nil {
		_ = json.Unmarshal(data, &report.Run)
	}
	f, err := os.Create(filepath.Join(runDir, "report.json"))
	if err != nil {
		return err
	}
	defer f.Close()
	enc := json.NewEncoder(f)
	enc.SetIndent("", "  ")
	return enc.Encode(report)
}
