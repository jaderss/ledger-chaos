package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

const ledgerSchema = `
	create table account (id uuid primary key, currency char(3), allow_overdraft boolean, balance_minor bigint);
	create table transfer (id uuid primary key, request_key text, status text, from_account uuid, to_account uuid, amount_minor bigint);
	create table posting (transfer_id uuid, account_id uuid, amount_minor bigint);
	create table outbox (transfer_id uuid, published_at timestamptz);
	insert into account values ('00000000-0000-0000-0000-00000000000a', 'EUR', true, -40),
	                           ('00000000-0000-0000-0000-00000000000b', 'EUR', false, 40);
	insert into transfer values ('00000000-0000-0000-0000-0000000000f1', 'k1', 'POSTED',
	    '00000000-0000-0000-0000-00000000000a', '00000000-0000-0000-0000-00000000000b', 40);
	insert into posting values ('00000000-0000-0000-0000-0000000000f1', '00000000-0000-0000-0000-00000000000a', -40),
	                           ('00000000-0000-0000-0000-0000000000f1', '00000000-0000-0000-0000-00000000000b', 40);
	insert into outbox values ('00000000-0000-0000-0000-0000000000f1', now());
`

const projectorSchema = `
	create table balance (account_id uuid primary key, balance_minor bigint);
	insert into balance values ('00000000-0000-0000-0000-00000000000a', -40), ('00000000-0000-0000-0000-00000000000b', 40);
`

const event = `{"eventId":"e1","transferId":"00000000-0000-0000-0000-0000000000f1","postings":[` +
	`{"accountId":"00000000-0000-0000-0000-00000000000a","amountMinor":-40},` +
	`{"accountId":"00000000-0000-0000-0000-00000000000b","amountMinor":40}]}`

func dsnOf(t *testing.T, db *pgx.Conn) string {
	t.Helper()
	cfg := db.Config()
	return fmt.Sprintf("postgres://%s:%s@%s:%d/%s", cfg.User, cfg.Password, cfg.Host, cfg.Port, cfg.Database)
}

func runFixture(t *testing.T, commit bool) (string, *bytes.Buffer, bool, error) {
	t.Helper()
	brokers := os.Getenv("TEST_KAFKA_BROKERS")
	if brokers == "" {
		t.Skip("TEST_KAFKA_BROKERS not set")
	}
	ledger := tempDatabase(t, ledgerSchema)
	projector := tempDatabase(t, projectorSchema)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	k, err := NewKafka(strings.Split(brokers, ","))
	if err != nil {
		t.Fatal(err)
	}
	defer k.Close()
	topic := fmt.Sprintf("run-test-%d", time.Now().UnixNano())
	if _, err := k.admin.CreateTopics(ctx, 1, 1, nil, topic, topic+".DLT"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { k.admin.DeleteTopics(context.Background(), topic, topic+".DLT") })
	if err := k.client.ProduceSync(ctx, &kgo.Record{Topic: topic, Value: []byte(event)}).FirstErr(); err != nil {
		t.Fatal(err)
	}
	if commit {
		offsets := kadm.Offsets{}
		offsets.Add(kadm.Offset{Topic: topic, Partition: 0, At: 1})
		if _, err := k.admin.CommitOffsets(ctx, topic+"-group", offsets); err != nil {
			t.Fatal(err)
		}
	}
	dir := t.TempDir()
	history := `{"op":"transfer","key":"k1","phase":"invoke","body":{"from":"00000000-0000-0000-0000-00000000000a","to":"00000000-0000-0000-0000-00000000000b","amount":40,"currency":"EUR"}}
{"op":"transfer","key":"k1","phase":"ok","status":201,"transferId":"00000000-0000-0000-0000-0000000000f1","transferStatus":"POSTED"}
`
	_ = os.WriteFile(filepath.Join(dir, "history.jsonl"), []byte(history), 0o644)
	_ = os.WriteFile(filepath.Join(dir, "run.json"), []byte(`{"seed":7}`), 0o644)
	for name, value := range map[string]string{
		"RUN_DIR": dir, "LEDGER_DSN": dsnOf(t, ledger), "PROJECTOR_DSN": dsnOf(t, projector), "KAFKA_BROKERS": brokers,
		"TOPIC": topic, "DLT_TOPIC": topic + ".DLT", "GROUP": topic + "-group", "QUIET_TIMEOUT": "1s",
	} {
		t.Setenv(name, value)
	}
	var out bytes.Buffer
	ok, err := run(&out)
	return dir, &out, ok, err
}

func TestRunPassesACleanSystemAndWritesTheReport(t *testing.T) {
	dir, out, ok, err := runFixture(t, true)
	if err != nil || !ok || !strings.Contains(out.String(), "verdict: correct passes every invariant") {
		t.Fatalf("ok %v, err %v\n%s", ok, err, out)
	}
	data, _ := os.ReadFile(filepath.Join(dir, "report.json"))
	var report struct {
		Locking string         `json:"locking"`
		Stats   Stats          `json:"stats"`
		OK      bool           `json:"ok"`
		Quiet   bool           `json:"quiet"`
		Run     map[string]any `json:"run"`
		Results []Result       `json:"results"`
	}
	if err := json.Unmarshal(data, &report); err != nil || !report.OK || !report.Quiet || report.Locking != "ordered" || report.Stats.OK != 1 || report.Run["seed"] != 7.0 || len(report.Results) != 10 {
		t.Fatalf("unexpected report %s", data)
	}
}

func TestRunFailsI8AndI9WhenTheProjectorNeverCatchesUp(t *testing.T) {
	_, out, ok, err := runFixture(t, false)
	if err != nil || ok {
		t.Fatalf("ok %v, err %v\n%s", ok, err, out)
	}
	if !strings.Contains(out.String(), "I8   FAIL") || !strings.Contains(out.String(), "projector lag 1 after 1s") {
		t.Fatalf("unexpected output:\n%s", out)
	}
}

func TestRunRefusesAnUnknownMode(t *testing.T) {
	t.Setenv("LEDGER_MODE", "corect")
	if _, err := run(&bytes.Buffer{}); err == nil || !strings.Contains(err.Error(), "corect") {
		t.Fatalf("expected an unknown mode error, got %v", err)
	}
}

func TestRunNeedsAHistory(t *testing.T) {
	t.Setenv("RUN_DIR", t.TempDir())
	if _, err := run(&bytes.Buffer{}); err == nil {
		t.Fatal("expected an error without history.jsonl")
	}
}
