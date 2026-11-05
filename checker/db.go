package main

import (
	"context"

	"github.com/jackc/pgx/v5"
)

func loadLedger(ctx context.Context, db *pgx.Conn, s *Snapshot) error {
	var err error
	if s.Transfers, err = collect(ctx, db,
		"select id::text, request_key, status, from_account::text, to_account::text, amount_minor from transfer",
		func(r pgx.Rows) (Transfer, error) {
			var t Transfer
			return t, r.Scan(&t.ID, &t.Key, &t.Status, &t.From, &t.To, &t.Amount)
		}); err != nil {
		return err
	}
	if s.Accounts, err = collect(ctx, db, "select id::text, allow_overdraft, balance_minor from account",
		func(r pgx.Rows) (Account, error) {
			var a Account
			return a, r.Scan(&a.ID, &a.Overdraft, &a.Balance)
		}); err != nil {
		return err
	}
	if s.CurrencySums, err = sums(ctx, db, `select a.currency, sum(p.amount_minor)::bigint
		from posting p join account a on a.id = p.account_id group by a.currency`); err != nil {
		return err
	}
	if s.PostingSums, err = sums(ctx, db,
		"select account_id::text, sum(amount_minor)::bigint from posting group by account_id"); err != nil {
		return err
	}
	s.TransferPostings = map[string]map[string]int64{}
	if _, err = collect(ctx, db,
		"select transfer_id::text, account_id::text, sum(amount_minor)::bigint from posting group by 1, 2",
		func(r pgx.Rows) (struct{}, error) {
			var transfer, account string
			var amount int64
			if err := r.Scan(&transfer, &account, &amount); err != nil {
				return struct{}{}, err
			}
			if s.TransferPostings[transfer] == nil {
				s.TransferPostings[transfer] = map[string]int64{}
			}
			s.TransferPostings[transfer][account] = amount
			return struct{}{}, nil
		}); err != nil {
		return err
	}
	if s.Outbox, err = collect(ctx, db, "select transfer_id::text, published_at is not null from outbox",
		func(r pgx.Rows) (OutboxRow, error) {
			var o OutboxRow
			return o, r.Scan(&o.TransferID, &o.Published)
		}); err != nil {
		return err
	}
	return db.QueryRow(ctx,
		"select deadlocks from pg_stat_database where datname = current_database()").Scan(&s.Deadlocks)
}

func loadProjector(ctx context.Context, db *pgx.Conn, s *Snapshot) error {
	var err error
	s.ProjectorBalances, err = sums(ctx, db, "select account_id::text, balance_minor from balance")
	return err
}

func unpublished(ctx context.Context, db *pgx.Conn) (int64, error) {
	var n int64
	return n, db.QueryRow(ctx, "select count(*) from outbox where published_at is null").Scan(&n)
}

func collect[T any](ctx context.Context, db *pgx.Conn, sql string, scan func(pgx.Rows) (T, error)) ([]T, error) {
	rows, err := db.Query(ctx, sql)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, func(r pgx.CollectableRow) (T, error) { return scan(r.(pgx.Rows)) })
}

func sums(ctx context.Context, db *pgx.Conn, sql string) (map[string]int64, error) {
	out := map[string]int64{}
	_, err := collect(ctx, db, sql, func(r pgx.Rows) (struct{}, error) {
		var key string
		var amount int64
		if err := r.Scan(&key, &amount); err != nil {
			return struct{}{}, err
		}
		out[key] = amount
		return struct{}{}, nil
	})
	return out, err
}
