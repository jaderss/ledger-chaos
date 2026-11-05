package main

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

func tempDatabase(t *testing.T, schema string) *pgx.Conn {
	t.Helper()
	dsn := os.Getenv("TEST_POSTGRES_DSN")
	if dsn == "" {
		t.Skip("TEST_POSTGRES_DSN not set")
	}
	ctx := context.Background()
	admin, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	name := fmt.Sprintf("checker_test_%d", time.Now().UnixNano())
	if _, err := admin.Exec(ctx, "create database "+name); err != nil {
		t.Fatal(err)
	}
	cfg, _ := pgx.ParseConfig(dsn)
	cfg.Database = name
	db, err := pgx.ConnectConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		db.Close(ctx)
		_, _ = admin.Exec(ctx, "drop database "+name)
		admin.Close(ctx)
	})
	if _, err := db.Exec(ctx, schema); err != nil {
		t.Fatal(err)
	}
	return db
}

func TestLoadLedgerReadsEverySnapshotField(t *testing.T) {
	db := tempDatabase(t, `
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
		insert into outbox values ('00000000-0000-0000-0000-0000000000f1', null);
	`)
	ctx := context.Background()
	var s Snapshot
	if err := loadLedger(ctx, db, &s); err != nil {
		t.Fatal(err)
	}
	b := "00000000-0000-0000-0000-00000000000b"
	if len(s.Transfers) != 1 || s.Transfers[0].Key != "k1" || len(s.Accounts) != 2 || s.CurrencySums["EUR"] != 0 ||
		s.PostingSums[b] != 40 || s.TransferPostings["00000000-0000-0000-0000-0000000000f1"][b] != 40 ||
		len(s.Outbox) != 1 || s.Outbox[0].Published {
		t.Fatalf("unexpected snapshot %+v", s)
	}
	if n, err := unpublished(ctx, db); err != nil || n != 1 {
		t.Fatalf("unpublished %d, %v", n, err)
	}
}

func TestLoadProjectorReadsBalances(t *testing.T) {
	db := tempDatabase(t, `
		create table balance (account_id uuid primary key, balance_minor bigint);
		insert into balance values ('00000000-0000-0000-0000-00000000000a', -7);
	`)
	var s Snapshot
	if err := loadProjector(context.Background(), db, &s); err != nil {
		t.Fatal(err)
	}
	if s.ProjectorBalances["00000000-0000-0000-0000-00000000000a"] != -7 {
		t.Fatalf("unexpected balances %v", s.ProjectorBalances)
	}
}

func TestKafkaEventsCountAndLag(t *testing.T) {
	brokers := os.Getenv("TEST_KAFKA_BROKERS")
	if brokers == "" {
		t.Skip("TEST_KAFKA_BROKERS not set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	k, err := NewKafka(strings.Split(brokers, ","))
	if err != nil {
		t.Fatal(err)
	}
	defer k.Close()
	topic := fmt.Sprintf("checker-test-%d", time.Now().UnixNano())
	if _, err := k.admin.CreateTopic(ctx, 2, 1, nil, topic); err != nil {
		t.Fatal(err)
	}
	defer k.admin.DeleteTopics(context.Background(), topic)
	if events, err := k.Events(ctx, topic); err != nil || len(events) != 0 {
		t.Fatalf("empty topic gave %v, %v", events, err)
	}
	for i := range 3 {
		value := fmt.Sprintf(`{"eventId":"e%d","transferId":"t%d","postings":[{"accountId":"a","amountMinor":-1},{"accountId":"b","amountMinor":1}]}`, i, i)
		if err := k.client.ProduceSync(ctx, &kgo.Record{Topic: topic, Key: []byte(fmt.Sprint(i)), Value: []byte(value)}).FirstErr(); err != nil {
			t.Fatal(err)
		}
	}

	events, err := k.Events(ctx, topic)
	if err != nil || len(events) != 3 || events[0].Postings["b"] != 1 {
		t.Fatalf("events %v, %v", events, err)
	}
	if n, err := k.Count(ctx, topic); err != nil || n != 3 {
		t.Fatalf("count %d, %v", n, err)
	}
	group := topic + "-group"
	if lag, err := k.Lag(ctx, group, topic); err != nil || lag != 3 {
		t.Fatalf("lag before commit %d, %v", lag, err)
	}
	ends, _ := k.admin.ListEndOffsets(ctx, topic)
	offsets := kadm.Offsets{}
	ends.Each(func(o kadm.ListedOffset) {
		offsets.Add(kadm.Offset{Topic: o.Topic, Partition: o.Partition, At: o.Offset})
	})
	if _, err := k.admin.CommitOffsets(ctx, group, offsets); err != nil {
		t.Fatal(err)
	}
	if lag, err := k.Lag(ctx, group, topic); err != nil || lag != 0 {
		t.Fatalf("lag after commit %d, %v", lag, err)
	}
}
