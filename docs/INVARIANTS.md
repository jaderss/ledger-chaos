# Invariants

What the checker proves after each run, how it checks it, and which naive mode breaks it. The examples are real checker output from the runs in `results/sample`.

## What the checker reads

| Source | What it holds |
|---|---|
| `history.jsonl` | Every request the runner sent and what came back, one line per event |
| Ledger database | Transfers, postings, accounts, the outbox, and `pg_stat_database` |
| Projector database | The balance per account, built from the topic |
| `ledger.transfers` topic | Every event, read from offset zero with no consumer group |

Each request in the history ends in one of three outcomes:

| Outcome | Meaning |
|---|---|
| `ok` | `201` for a new transfer, `200` for a replay. Always `POSTED` |
| `fail` | Any `4xx`, a definite refusal: `422` insufficient funds or key reused with another body, `404`, `400` |
| `unknown` | Timeout, reset, any `5xx`. The transfer may or may not exist |

A client that gets `unknown` retries with the same `Idempotency-Key`. That is the case a ledger must survive: the commit happened, the answer did not arrive.

Before it reads anything, the checker waits until the system is quiet: no unpublished outbox rows and no projector lag. If that never happens within `QUIET_TIMEOUT`, I8 and I9 fail with the reason.

Each check is a pure function over one snapshot of these four sources. The checker's tests corrupt one field of a clean snapshot per invariant and assert that exactly that invariant fails.

## I1. The sum of all postings per currency is zero

Every transfer writes two postings, `-amount` on the source and `+amount` on the destination. Money is not created or destroyed, so the postings of a currency always sum to zero.

```sql
select a.currency, sum(p.amount_minor)::bigint
from posting p join account a on a.id = p.account_id
group by a.currency
```

Fails when any row is not zero. No naive mode breaks I1, since every mode writes both postings in one transaction. A deferred trigger in the schema also rejects the commit of a transfer whose postings do not sum to zero.

## I2. Each account balance equals the sum of its postings

`account.balance_minor` is a cache of the postings, updated in the same transaction. Postings are append only, so they are the truth.

```sql
select id::text, allow_overdraft, balance_minor from account
select account_id::text, sum(amount_minor)::bigint from posting group by account_id
```

Fails for every account where the two differ. Broken by `read-modify-write`, which reads the balance without a lock and writes back its own result, so two concurrent transfers lose one update:

```
I2   FAIL     20  each account balance equals the sum of its postings
            account 213afc19-0ee7-4342-940d-92e862adf607 stores 275370, postings sum to -52980
```

## I3. No account without overdraft goes below zero

Reads the sum of postings, not the stored balance. The check constraint on `balance_minor` keeps the stored value at zero or above, so a lost update only shows in the postings.

Fails for every account with `allow_overdraft = false` and a negative sum. Also broken by `read-modify-write`: both transfers read the same balance, both see enough money, both post.

```
I3   FAIL      5  no account without overdraft goes below zero
            account a74093f0-1cc6-4c13-882e-cab39001625d without overdraft has postings summing to -65509
```

## I4. A transfer acknowledged to the client exists exactly once

```sql
select id::text, request_key, status, from_account::text, to_account::text, amount_minor from transfer
```

`transfer.request_key` always holds the client's key, and it is not unique, so a duplicate can exist and be counted. For every key with an `ok` answer in the history, exactly one `POSTED` transfer must have that key.

Broken by `no-idempotency`, which skips the key claim. The first attempt commits, the answer times out, the retry posts again:

```
I4   FAIL   1857  an acknowledged transfer exists exactly once
            key 003957a8-dcfa-410a-86bf-cb4caaf6e601 was acknowledged and has 2 posted transfers
```

## I5. A transfer refused with a definite error does not exist

For every key with a `fail` answer and no `ok`, no `POSTED` transfer has that key. A `422` for a key reused with another body does not count here, I7 covers it.

Broken by `no-idempotency`. One attempt timed out after its commit, and a later attempt with the same key was refused. The client sees a refusal, and the money moved:

```
I5   FAIL      6  a transfer refused with a definite error does not exist
            key 0ab4664e-d48e-4125-bafc-7a49c56f3932 was refused with 422 and has 1 posted transfers
```

## I6. A transfer with unknown outcome exists at most once

For every key whose answers were all `unknown`, zero or one transfer has that key. Zero is fine: the request may never have reached the database.

Broken by `no-idempotency`, when every retry timed out and more than one committed:

```
I6   FAIL     49  a transfer with unknown outcome exists at most once
            key 013129e6-2e5e-45f7-b04f-540c8be8f907 had only unknown outcomes and has 2 transfers
```

## I7. Same key and body give the same answer, another body changes nothing

Two parts, both from the history:

* Every definite answer for a key names the same transfer id and status as the first one.
* The runner sometimes reuses a key with a different body on purpose. That request must get `422 idempotency-key-mismatch`, and no transfer may have its body.

Broken by `no-idempotency`, where each retry creates a new transfer with a new id:

```
I7   FAIL   1991  same key and body give the same answer, another body changes nothing
            key 003957a8-dcfa-410a-86bf-cb4caaf6e601 answered f2f7dcf1-7aa6-4d96-85d0-a96ef85206e1 POSTED, then 9b179d7f-b2dc-41e4-b751-03d311d441c4 POSTED
```

## I8. Every posted transfer reaches the topic, and every event matches a posted transfer

Reads the whole topic and compares it with the ledger by `transferId`:

* Every `POSTED` transfer has at least one event. Duplicates are allowed, the relay is at least once.
* Every event matches a `POSTED` transfer, with the same postings.
* In `correct` mode also: one outbox row per `POSTED` transfer, none unpublished.

```sql
select transfer_id::text, account_id::text, sum(amount_minor)::bigint from posting group by 1, 2
select transfer_id::text, published_at is not null from outbox
```

Broken by `dual-write`, which commits, then sends to Kafka. While Kafka is down, the send fails after the commit and the event is gone:

```
I8   FAIL    407  every posted transfer reaches the topic, and only those
            posted transfer 8a58c6f3-9d36-4733-a9f4-addcbb3fd3aa is not on the topic
```

## I9. Projector balances equal the ledger's sums of postings

```sql
select account_id::text, balance_minor from balance
```

Run against the projector database. For every account, the projected balance equals the ledger's sum of postings. An account with no projector row counts as zero. Also fails when the dead letter topic `ledger.transfers.DLT` has any record.

Compares with the postings, not `account.balance_minor`, so a wrong stored balance fails I2 only.

Broken by `dual-write`, since the projector never sees the lost events:

```
I9   FAIL     20  projector balances equal the ledger's sums of postings
            account 09078166-78c9-4a72-b024-1261c68eec17 projected 206576, ledger postings sum to 200080
```

## I11. No transfer transaction ends in a deadlock

```sql
select deadlocks, xact_commit, xact_rollback from pg_stat_database where datname = current_database()
```

`scripts/chaos.sh` calls `pg_stat_reset()` before the workload, so the counter covers this run only. Fails when it is above zero.

Broken by `unordered-locks`, which locks `from` and then `to`. Two transfers in opposite directions each hold one lock and wait for the other, and Postgres aborts one after `deadlock_timeout`:

```
I11  FAIL      1  no transfer transaction ends in a deadlock
            128 deadlocks in the ledger database
```

The correct mode locks both accounts in id order, so every transfer waits in the same direction.

## I10. Linearizable balance reads

Not built yet. It needs a linearizability checker over transfers and balance reads, a register per account.

## Verdict

The correct mode passes when every invariant passes. A naive mode passes when every invariant in its **must fail** set fails:

| Mode | Must fail |
|---|---|
| `correct` | (none) |
| `no-idempotency` | I4, I6, I7 |
| `dual-write` | I8, I9 |
| `unordered-locks` | I11 |
| `read-modify-write` | I2 |

If a naive mode stops failing, the checker or the scenario lost its teeth, and CI fails.
