# SDD: ledger-chaos (v1)

> **Status:** Draft.
> **Author:** Jader Santos.
> **Implements:** [`PLAN.md`](PLAN.md).
> **Open questions:** see [§12](#12-open-questions).

---

## 1. Context

Payment and ledger services fail in the gaps: a client timeout after the commit, a crash between the database write and the Kafka publish, a consumer that processes an event twice. The patterns that close those gaps are known: idempotency keys, transactional outbox, idempotent consumer, ordered locks. Most public examples show the happy path and never prove the patterns hold under failure.

## 2. Problem

There is no small, runnable project that injects real faults into a ledger and then proves, invariant by invariant, that the money is still right, and shows what goes wrong without each pattern.

## 3. Goals

* A ledger with idempotent transfers, outbox, relay and projector.
* A chaos runner that records every client call and every fault.
* A checker that verifies invariants I1 to I9 and I11 ([PLAN §2](PLAN.md#2-invariants)), I10 as a stretch goal.
* Naive modes that break known invariants, so the checker is proven to catch them.
* One command per scenario, results published, a short chaos run in CI.

## 4. Non-goals

* FX, fees, interest, statements, users, authentication.
* Throughput records. Latency and throughput are reported only to compare locking strategies.
* Multi-node Postgres and failover (see [§12 Q3](#12-open-questions)).
* A production deployment.

---

## 5. Proposed design

### 5.1 High level

```
                 HTTP + Idempotency-Key
  chaos  ------------------------------->  ledger  ---+
  (workload,                                           |  one transaction:
   nemesis,                                            |  transfer, postings,
   history)                                            v  balances, outbox
     |                                          toxiproxy ---> postgres (ledger db)
     | Docker API: kill, stop, pause                              ^
     | Toxiproxy API: latency, reset, cut                         | FOR UPDATE SKIP LOCKED
     v                                                            |
  every container                          relay  ----------------+
                                             |
                                             | idempotent producer, acks=all
                                             v
                                   toxiproxy ---> kafka (ledger.transfers)
                                                     |
                                                     v
                                                 projector ---> postgres (projector db)
                                                                 processed_event + balance

  checker: reads history.jsonl, faults.jsonl, both databases, the topic. One line per invariant.
```

### 5.2 Components

| Component | Responsibility | Where it lives |
|---|---|---|
| `ledger` | Transfer API, idempotency, locking, postings, outbox write | `services/ledger` |
| `relay` | Outbox polling and publish | `services/relay` |
| `projector` | Idempotent consumer, read model of balances | `services/projector` |
| `chaos` | Workload, nemesis (fault injector), history | `chaos` |
| `checker` | Invariant checks, report JSON | `checker` |
| `report` | Results page: every run, invariants per run, the locking comparison, one run in detail with its fault windows | `report` |

Inside `ledger` the code is layered, with a pure domain at the center. Not hexagonal.

| Package | Role | Depends on |
|---|---|---|
| `domain` | Values, ids, `TransferDecision.decide`: two accounts and an amount in, `Post` or `Reject` out. No Spring, JDBC or Jackson | Nothing |
| `app` | Runs the transaction around the decision: claim the key, load the accounts, decide, write, publish | `domain`, `persistence`, `messaging` |
| `persistence` | `LedgerDatabase` owns the Jdbi instance and every transaction. One store per table, a record over the transaction's Jdbi `Handle`, no interfaces in front. `LedgerStores` groups the five for one transaction | `domain` |
| `messaging` | `DirectPublisher`, the Kafka send of the `dual-write` mode. The correct mode never uses it | `domain` |
| `api` | Controllers, Problem Details, the correlation id filter | `app`, `domain`, `persistence` for reads |

`ArchitectureTest` enforces the arrows: `domain` imports no framework and none of the other packages, `persistence` and `messaging` never import `app`, `api` or each other, only `persistence` imports Jdbi, and no class uses Spring's `jdbc`, `dao` or `transaction` packages. `TransferSteps` holds the three steps that change between modes (§5.12): claim the key, load the accounts, publish the event. Each step is a small functional interface, so a mode is one line in `TransferServiceConfig`, not a class.

### 5.3 Data model: ledger database

```sql
create table account (
    id              uuid primary key,
    currency        char(3) not null,
    allow_overdraft boolean not null default false,
    balance_minor   bigint  not null default 0,
    constraint balance_not_negative check (allow_overdraft or balance_minor >= 0)
);

create table transfer (
    id              uuid primary key,
    request_key     text        not null,
    correlation_id  text        not null,
    from_account    uuid        not null references account (id),
    to_account      uuid        not null references account (id),
    amount_minor    bigint      not null check (amount_minor > 0),
    currency        char(3)     not null,
    status          text        not null check (status in ('POSTED', 'REJECTED')),
    reject_reason   text,
    created_at      timestamptz not null default now(),
    check (from_account <> to_account)
);

create index transfer_request_key on transfer (request_key);

create table idempotency (
    key          text  primary key,
    request_hash bytea not null,
    transfer_id  uuid  not null references transfer (id) deferrable initially deferred
);

create table posting (
    id           bigint generated always as identity primary key,
    transfer_id  uuid   not null references transfer (id),
    account_id   uuid   not null references account (id),
    amount_minor bigint not null check (amount_minor <> 0)
);

create index posting_account on posting (account_id);
create index posting_transfer on posting (transfer_id);

create table outbox (
    id           uuid primary key,
    seq          bigint generated always as identity unique,
    transfer_id  uuid        not null references transfer (id) unique,
    payload      jsonb       not null,
    created_at   timestamptz not null default now(),
    published_at timestamptz
);

create index outbox_unpublished on outbox (seq) where published_at is null;
```

Postings of a transfer must sum to zero. A deferred constraint trigger on `posting` checks it at commit, so a bug in Java fails the transaction instead of corrupting the ledger:

```sql
create constraint trigger posting_sum_zero
    after insert on posting
    deferrable initially deferred
    for each row execute function check_transfer_balanced();

create trigger posting_append_only
    before update or delete on posting
    for each statement execute function reject_change();
```

`posting` is append only, and the second trigger makes that a rule of the database, not only of the code.

`transfer.request_key` always holds the client key, with a plain index. Uniqueness lives in `idempotency`, which only the correct mode writes. That split lets the `no-idempotency` mode really post twice (§5.12), and lets the checker count transfers per client key in every mode.

### 5.4 Data model: projector database

```sql
create table processed_event (
    event_id     uuid primary key,
    processed_at timestamptz not null default now()
);

create table balance (
    account_id    uuid primary key,
    currency      char(3) not null,
    balance_minor bigint  not null
);
```

### 5.5 Transfer API

`POST /transfers`

```http
POST /transfers HTTP/1.1
Idempotency-Key: 7f1c2a9e-0d4b-4a59-9a57-3c2f8e1b6d10
X-Request-Id: 5d0e9b3a-61c4-4f1e-8a2b-7c9d0e4f1a23
Content-Type: application/json

{ "fromAccount": "0b5f...", "toAccount": "9c21...", "amountMinor": 2500, "currency": "EUR" }
```

| Case | Status | Body |
|---|---|---|
| New transfer posted | `201` | Transfer with `status: POSTED` |
| Same key, same body, transfer posted | `200`, header `Idempotent-Replayed: true` | Stored transfer, `status: POSTED` |
| Same key, same body, transfer rejected | `422`, header `Idempotent-Replayed: true` | Problem `insufficient-funds`, same as the first answer |
| Same key, other body | `422` | Problem `idempotency-key-mismatch` |
| Insufficient funds | `422` | Problem `insufficient-funds`. Stored as a `REJECTED` transfer, so a retry gets the same `422` |
| Currency differs from an account | `422` | Problem `currency-mismatch`. Not stored |
| Unknown account | `404` | Problem `account-not-found`. Not stored |
| Missing or malformed key | `400` | Problem `idempotency-key-required` |
| Invalid body, or `fromAccount` equal to `toAccount` | `400` | Validation problem, or `invalid-request`. Not stored |
| Postings that do not sum to zero (a bug) | `500` | Problem `ledger-integrity`. The deferred trigger rolls the transaction back |
| Database unavailable or timeout | `503` | Problem `unavailable`. Outcome unknown to the client |

Errors use `ProblemDetail` (RFC 9457), with `type` under `/problems/`. A rejected transfer's problem also carries `transferId` and `transferStatus`, so the chaos runner can match a replayed `422` to the first one. The field is not called `status`, because RFC 9457 already uses `status` for the HTTP code.

`X-Request-Id` is optional. `CorrelationIdFilter` generates one when it is missing, puts it in MDC, and returns it in the response header. The ledger stores it on the transfer and in the event, and the relay and projector log it, so one id follows a transfer from the request to the read model.

Other endpoints: `GET /transfers/{id}`, `GET /transfers?idempotencyKey=` (used by the checker for unknown outcomes), `GET /accounts/{id}`, `POST /accounts` (setup only).

`POST /accounts` takes an id, a currency and `allowOverdraft`, never an opening balance. It is idempotent on the id: `201` when created, `200` when the same account exists, `409` `account-conflict` when it exists with other settings. A balance that does not come from postings would break I2. Money enters through one **mint** account per currency, created with `allowOverdraft = true`. Before the faults start, the chaos runner sends one funding transfer of `FUNDING_MINOR` from the mint to each workload account, through `POST /transfers` and recorded in the history like any other call. The mint goes negative by the total funded, so I1 still holds, and the projector sees the funding events like any other. The workload never uses the mint after that.

### 5.6 Transfer algorithm

One transaction, `READ COMMITTED`.

1. Hash the request: SHA-256 over the parsed `TransferRequest` fields in a fixed order (`fromAccount`, `toAccount`, `amountMinor`, `currency`). Not the raw body, so whitespace or field order never count as a mismatch.
2. `insert into idempotency (key, request_hash, transfer_id) values (:key, :hash, :newTransferId) on conflict (key) do nothing returning transfer_id`.
3. No row returned: load the `idempotency` row and its transfer. Hash equal: return the transfer as a replay. Hash different: `422`. A concurrent duplicate waits on the primary key here until the first transaction ends, then takes this branch. If the first transaction rolled back, the insert succeeds and this request goes on as new.
4. `select ... from account where id in (:from, :to) order by id for update`. Id order means two transfers on the same pair never deadlock. An unknown account (`404`) or a currency mismatch (`422`) throws here. The transaction rolls back with the `idempotency` row, so nothing is stored, and a retry gets the same answer because accounts never change currency and are never deleted.
5. The domain decides: `TransferDecision` is a sealed type with `Post` and `Reject(reason)`.
6. `Reject`: insert the transfer with `REJECTED`, commit, `422`.
7. `Post`: insert the transfer with `POSTED`, insert two postings (`-amount`, `+amount`), update both balances with `Math.addExact` results, insert the outbox row, commit, `201`.

The foreign key from `idempotency` to `transfer` is deferred, so step 2 can claim the key before the transfer row exists.

Timeouts: `STATEMENT_TIMEOUT` on the server, `DB_SOCKET_TIMEOUT_S` (pgjdbc `socketTimeout`) on the client. When Postgres is paused, the server enforces nothing, and only the client-side timeout ends the call with a `503`. The relay and the projector set the same `socketTimeout`, so a paused database never leaves them waiting on a dead socket.

Every transaction runs through `LedgerDatabase.inTransaction(isolation, stores -> ...)`, a Jdbi transaction. `READ_COMMITTED` is the Postgres default and adds nothing. `SERIALIZABLE` runs `set transaction isolation level serializable` as the transaction's first statement. Asking the driver for the level instead costs a `SHOW TRANSACTION ISOLATION LEVEL` round trip each time Jdbi checks, sets or restores it: six per transfer, each one its own commit, and the correct mode on `all` fell from 1,661 to 742 ok/s. `IsolationTest` checks the level each transaction really runs at. Jdbi errors leave `LedgerDatabase` as a sealed `DatabaseException`, chosen by the SQLState class: `23` is `IntegrityViolation` and answers `500 ledger-integrity`, `40` (`40001` serialization failure, `40P01` deadlock) is `ConcurrencyConflict`, and anything else is `Unavailable`. Both of the last two answer `503`, outcome unknown.

`LEDGER_LOCKING=serializable` replaces step 4 with `SERIALIZABLE` isolation and no row locks. The retry wraps the whole transaction: a loop around `inTransaction` catches `ConcurrencyConflict` up to `SERIALIZABLE_RETRIES` times, with no backoff. A catch inside the transaction would retry in a transaction that Postgres already rolled back. `TransactionPolicy` holds the isolation level and the retry budget, so both locking strategies run through the same `LedgerTransferService`. `ordered` has a budget of zero. Serializable only runs with the correct mode, and `LedgerProperties` refuses any other combination at startup.

Step 7 updates the two balances in account id order, in every mode. Without row locks, those `update`s are the first locks a serializable transaction takes. In request order, two transfers in opposite directions deadlock, and every deadlock waits `deadlock_timeout` (1 s) before Postgres picks a victim. The first `SerializableLockingTest` run took 348 s and ran out of pooled connections. With the updates in id order, the same test takes 2 s. `AccountId` sorts byte by byte, like Postgres sorts `uuid`. `UUID.compareTo` compares signed longs and gives another order.

### 5.7 Event contract

Topic `ledger.transfers`, key is the transfer id.

```json
{
  "eventId": "c8d4c0a2-5b8e-4a54-b3f5-0a1f0f3c9d77",
  "type": "TransferPosted",
  "transferId": "e2b9...",
  "correlationId": "5d0e9b3a-61c4-4f1e-8a2b-7c9d0e4f1a23",
  "occurredAt": "2026-11-04T18:22:05.114Z",
  "currency": "EUR",
  "postings": [
    { "accountId": "0b5f...", "amountMinor": -2500 },
    { "accountId": "9c21...", "amountMinor": 2500 }
  ]
}
```

`eventId` is the outbox row id. A republished row carries the same `eventId`, which is what the projector dedups on. `occurredAt` is the transaction's `now()` from Postgres, so the ledger has one time source and Java never calls `Instant.now()` for it.

### 5.8 Relay loop

```sql
begin;
select id, transfer_id, payload from outbox
 where published_at is null
 order by seq
 limit :batch
 for update skip locked;
-- send each payload, wait for every ack
update outbox set published_at = now() where id = any(:ids);
commit;
```

A crash after the send and before the commit republishes the batch. That is expected and covered by the projector. `SKIP LOCKED` lets more than one relay run without double claiming a row. The relay selects every unpublished row, not rows above a `seq` cursor: a transaction that commits after one with a higher `seq` would fall behind a cursor and never be sent.

The relay sends the payload as stored, with the transfer id as key and the correlation id as a header. It never rebuilds the JSON, so the event on the topic is the one the ledger wrote in the transfer's transaction.

The relay holds the transaction while it waits for acks, so a Kafka outage must not keep it open. Producer `delivery.timeout.ms`, `request.timeout.ms` and `max.block.ms` stay below `idle_in_transaction_session_timeout`, set on the relay's database role. When Kafka is down the send fails inside that limit, the transaction rolls back, the rows stay unpublished, and the next poll tries again. The bound is `KAFKA_MAX_BLOCK_MS + KAFKA_DELIVERY_TIMEOUT_MS < IDLE_IN_TX_TIMEOUT` (3 s + 5 s against 10 s in `.env`), because one batch can block on metadata and then wait for delivery. The relay checks it at startup and refuses to start when it does not hold.

### 5.9 Projector loop

Per record, one transaction:

1. `insert into processed_event (event_id) values (:id) on conflict do nothing`.
2. Inserted: upsert each posting into `balance` (`balance_minor = balance.balance_minor + excluded.balance_minor`), in account id order. Events from different partitions run in parallel, and two of them on the same pair of accounts would deadlock if one upserted `from` first and the other `to` first.
3. Commit, then acknowledge the offset (`AckMode.MANUAL_IMMEDIATE`).

A crash after the commit and before the ack replays the record, and step 1 skips it.

The consumer group starts with `auto.offset.reset=earliest`. `session.timeout.ms` is `KAFKA_SESSION_TIMEOUT_MS` (10 s), not the Kafka default of 45 s. A killed projector cannot leave the group, so its partitions stay with the dead member until the session times out, and 45 s against a `QUIET_TIMEOUT` of 60 s would fail I9 after a late `kill-projector` for no real reason. With Kafka's default, `latest`, a projector that joins after the first events skips them, and I9 fails for a reason that has nothing to do with the patterns.

Error handling: the default Spring Kafka `DefaultErrorHandler` retries 9 times, then commits the offset and the event is gone. The projector sets a `DefaultErrorHandler` with an unlimited `ExponentialBackOff` for database errors, so a paused database stops consumption instead of skipping records. Only a record that cannot be parsed goes to `ledger.transfers.DLT`. Not parsed includes an event whose postings do not sum to zero: `TransferPosted` refuses it in its constructor, because retrying it can never succeed. The checker reads the DLT, and any record there fails I9. A record that parses but that Postgres refuses stays in the backoff loop and is logged with its transfer id and offset on every retry, so a stuck partition shows up as an I9 quiet timeout with the record named in the projector log.

### 5.10 Chaos runner

Workload:

* `ACCOUNTS` accounts in one currency, every `OVERDRAFT_EVERY`th with overdraft. A small number on purpose, to force lock contention.
* `WORKERS` virtual threads send transfers with random pairs and amounts from 1 to `AMOUNT_MAX_MINOR`, each with a new UUID key.
* Timeout, connection error or `5xx`: record `unknown`, retry with the same key up to `RETRIES` times.
* `DUPLICATE_RATE` of transfers are sent twice in parallel with the same key.
* `MISMATCH_RATE` of transfers reuse a key with another body. Only keys that already got a definite `201` or `200` are reused. A key whose outcome is still unknown may never have committed, and then the mismatch body would become the transfer in a correct ledger and fail I7 for the wrong reason.
* `READERS` virtual threads read balances for I10. Not built yet, they come with the linearizability check (§12 Q4).

Every request has its own `CLIENT_TIMEOUT`, not the run's deadline, so the end of the run never cuts a request short. The HTTP client is `java.net.http` on HTTP/1.1, which keeps every idle connection for reuse. The first runner, in Go, kept two idle connections per host, and at about 2,400 requests per second it ran out of local ports 42 s into a run: thousands of `unknown` answers the ledger never saw. `LedgerClientTest` checks that 16 workers sending 800 transfers open at most 32 connections. Jackson is the runner's only library. The ledger and Toxiproxy go through `java.net.http`, and the Docker Engine API is HTTP/1.0 over the Unix socket, so the daemon closes each connection and the client reads to the end of the stream. The `chaos` container runs as root: the socket belongs to root and a `docker` group whose id differs per host, and access to the socket is root on the host anyway.

History, one JSON line per event:

```json
{"t":1730744525114,"worker":7,"op":"transfer","key":"7f1c...","phase":"invoke","body":{"from":"0b5f...","to":"9c21...","amount":2500,"currency":"EUR"}}
{"t":1730744525161,"worker":7,"op":"transfer","key":"7f1c...","phase":"ok","status":201,"transferId":"e2b9...","transferStatus":"POSTED"}
{"t":1730744526002,"worker":3,"op":"transfer","key":"a90d...","phase":"unknown","error":"context deadline exceeded"}
{"t":1730744527310,"worker":5,"op":"transfer","key":"7f1c...","phase":"invoke","mismatch":true,"body":{"from":"0b5f...","to":"9c21...","amount":2501,"currency":"EUR"}}
{"t":1730744527322,"worker":5,"op":"transfer","key":"7f1c...","phase":"fail","mismatch":true,"status":422,"problem":"idempotency-key-mismatch"}
```

Every attempt writes an `invoke` and one result. `ok` is a `201` or `200`, `fail` is any `4xx`, `unknown` is a timeout, a connection error or a `5xx`. A mismatch attempt carries `mismatch: true` on both lines, so the checker keeps it apart from the key's own answers.

Nemesis: every `FAULT_INTERVAL` it picks a fault from the scenario, applies it for `FAULT_DURATION`, heals it, and writes `faults.jsonl`.

| Fault | How |
|---|---|
| `kill-ledger` | Docker API `POST /containers/{id}/kill` with `SIGKILL`, then start |
| `kill-relay` | Same, on `relay` |
| `kill-projector` | Same, on `projector` |
| `pause-db` | Docker API `pause` and `unpause` on Postgres |
| `restart-db` | Docker API `restart` on Postgres |
| `db-cut` | Toxiproxy: disable the Postgres proxy |
| `db-latency` | Toxiproxy `latency` toxic of `DB_LATENCY` (1.5 s), above the 1 s client timeout and below the 5 s socket timeout, so transactions still commit after the client gave up |
| `db-reset` | Toxiproxy `reset_peer` toxic |
| `kafka-down` | Docker API `stop` and `start` on Kafka |

Docker Engine API through the Unix socket with `net/http`, no Docker SDK. Containers are found by their Compose labels. The runner heals every fault in the catalog before the workload starts and again after it stops, so a run that crashed halfway never leaves the next one with a paused database or a disabled proxy.

A **scenario** is a named set of faults for the nemesis. `make chaos MODE=... SCENARIO=...` runs one pair.

| Scenario | Faults | Why it exists |
|---|---|---|
| `contention` | (none) | Lock contention only. Breaks `read-modify-write` and `unordered-locks`, and is the baseline for the correct mode |
| `db-latency` | `db-latency` | Client timeouts on transfers that still commit, so retries hit keys with unknown outcome. Breaks `no-idempotency` on I6, only when `FAULT_DURATION` is longer than `(RETRIES + 1) × CLIENT_TIMEOUT` (see below) |
| `kafka-down` | `kafka-down` | Breaks `dual-write`: the transfer commits and the publish fails |
| `kills` | `kill-ledger`, `kill-relay`, `kill-projector` | Crashes between the steps of §5.6, §5.8 and §5.9 |
| `db-faults` | `pause-db`, `restart-db`, `db-cut`, `db-reset` | Database gone or slow in every way the catalog has |
| `all` | Every fault above | Long runs on the laptop and the correct mode in CI |

I6 only checks keys the client gave up on: every attempt `unknown`, none definite. A fault shorter than the retry budget heals before the last retry, so every key ends with a definite answer and I6 passes with nothing to check. The first `no-idempotency` run with the 3 s default had zero such keys and passed I6. With an 8 s fault, 70 keys had more than one transfer and I6 failed. `db-latency` runs use `FAULT_DURATION=8s` and `FAULT_INTERVAL=12s` against the default 6 attempts of 1 s, and the checker prints the number of keys never answered, so an I6 with nothing to check shows in the output.

End of run: stop the workload, heal everything, write `run.json` with the seed and the settings. The checker then waits until `outbox` has no unpublished rows and the projector consumer group has no lag, with `QUIET_TIMEOUT` as the limit. A timeout is reported as an I8 and I9 failure. The wait lives in the checker because the checker already reads both databases and Kafka, and the runner stays a client.

`scripts/chaos.sh` runs one mode and scenario from a clean stack: `docker compose down`, then infrastructure, `db-init`, topics, the three services, `pg_stat_reset()` on the ledger database, the runner, the checker. The stack starts clean every time because a naive run leaves wrong data behind, and the next run would fail I1 to I3 on it.

### 5.11 Checker

| Invariant | Check |
|---|---|
| I1 | `select currency, sum(p.amount_minor) from posting p join account a on a.id = p.account_id group by currency` is zero for every currency |
| I2 | `account.balance_minor` equals the sum of its postings, every account |
| I3 | No account with `allow_overdraft = false` has a negative sum of postings. The check constraint already guards the stored balance, so the checker reads the postings, where a lost update shows the overspend |
| I4 | Every key with an `ok` 201 or 200 `POSTED` in the history has exactly one `POSTED` transfer with that `request_key` |
| I5 | Every key with a definite `4xx` and no later `ok` has no `POSTED` transfer with that `request_key`. `idempotency-key-mismatch` answers do not count here, I7 covers them |
| I6 | Every key with only `unknown` results has zero or one transfer with that `request_key` |
| I7 | Replays returned the same transfer id and status as the first `ok`. Mismatches returned `422` and no transfer has their body |
| I8 | Every `POSTED` transfer is on the topic at least once, matched by `transferId`. Every event on the topic matches a `POSTED` transfer, with the same postings. In `correct` mode also: one outbox row per `POSTED` transfer and none unpublished |
| I9 | Projector `balance` equals the ledger's sum of postings for every account, and `ledger.transfers.DLT` is empty. An account with no projector row counts as zero. The sum of postings, not `account.balance_minor`, so a wrong stored balance fails I2 only |
| I10 | Porcupine model of a register per account over transfers and reads (stretch goal) |
| I11 | `pg_stat_database.deadlocks` for the ledger database is zero. `scripts/chaos.sh` calls `pg_stat_reset()` on that database before the workload |

Output: one line per invariant with pass or fail, a count and up to five examples, a stats line, a verdict line, plus `report.json` with the verdict, the stats and the run settings from `run.json`. The stats come from `history.jsonl` and `faults.jsonl`, workload only (the funding transfers are left out): distinct keys sent, ok answers per second, p50 and p99 latency of definite answers, answers per second by outcome, the fault windows in seconds from the start, and `xact_commit` and `xact_rollback` of the ledger database. The commit count includes the relay's polling, so the report compares rollbacks against ok answers, not against commits. The checker knows the **Must fail** set of each mode (§5.12) and judges the run itself: `correct` passes when every invariant passes, a naive mode passes when every invariant in its set fails. Exit code `0` on a passing verdict, `1` otherwise, so `make chaos` and `chaos.yml` need no logic of their own. An unknown `LEDGER_MODE` is an error before anything is loaded.

The checker is a plain Java module, `services/checker`, with no Spring context: Jdbi for both databases, the Kafka admin client for offsets and lag, and a consumer with `assign()` and no `group.id` to read the topic, so judging a run never joins or moves the projector's group. Each invariant is a pure function over a `Snapshot` of the history, both databases and the topic. The loaders are separate. That lets the tests corrupt one field of a clean snapshot per invariant and assert that exactly that invariant fails. The checker connects as `postgres` straight to port 5432 and to Kafka on the `INTERNAL` listener, never through Toxiproxy.

### 5.12 Naive modes

`LEDGER_MODE` picks the steps of the one `LedgerTransferService`. Each naive mode swaps one step of the correct mode and keeps the rest, so the whole difference is one line in `TransferServiceConfig`. Never on by default. `LEDGER_MODE` and `LEDGER_LOCKING` bind to enums in a `LedgerProperties` record with `@ConfigurationProperties` and `@Validated`, so a typo fails startup instead of running the default mode.

| Mode | Change | Scenario | Must fail |
|---|---|---|---|
| `correct` | Algorithm in §5.6 | Every scenario | (none) |
| `no-idempotency` | Key claim always succeeds without touching the `idempotency` table. A retry with the same key posts again, with its own `transfer` row and the same `request_key` | `db-latency` | I4, I6, I7 |
| `dual-write` | No outbox row. `DirectPublisher` sends to Kafka from the Jdbi handle's `afterCommit`, and a failed send is only logged | `kafka-down` | I8, I9 |
| `unordered-locks` | Locks `from`, then `to`, one `select ... for update` each | `contention` | I11 |
| `read-modify-write` | Reads balances without `for update`, writes computed values | `contention` | I2 |

A naive run passes when the checker fails at least its **Must fail** set. Other failures go in the report but do not change the verdict:

* `no-idempotency` also fails I5. A first attempt times out but commits, the retry posts again and drains the account, and a later retry gets `422` insufficient funds while the key already has a `POSTED` transfer.
* `read-modify-write` fails I3 when a lost update lets an account without overdraft overspend, and that depends on `FUNDING_MINOR` against the transfer amounts (§12 Q6).
* `read-modify-write` no longer fails I11. The first runs had deadlocks, because the two balance `update`s were the first locks the transaction took, in request order. Since step 7 updates in account id order (§5.6), it loses updates without deadlocking.

Every naive mode also has a Testcontainers test that shows its defect without the chaos runner. `NoIdempotencyModeTest` posts the same key twice. `DualWriteModeTest` reads the event from Kafka and finds no outbox row. `UnorderedLocksModeTest` and `ReadModifyWriteModeTest` hold a row lock in an open transaction, start a transfer on another thread, wait until it blocks in `pg_stat_activity`, then take a second lock for a deadlock, or commit a change to the same balance for a lost update. `TransferConcurrencyTest` runs the same two races on the correct mode and gets no deadlock and no lost update.

First runs, 60 s each unless noted, default settings otherwise, before the balance updates moved to id order. The Phase 5 runs are in §5.14.

| Mode | Scenario | Keys | Verdict |
|---|---|---|---|
| `correct` | `contention`, 30 s | 49,387 | Every invariant passes |
| `correct` | `db-latency`, 8 s faults | 45,467 | Every invariant passes, 256 keys never answered |
| `correct` | `kafka-down` | 135,133 | Every invariant passes |
| `correct` | `db-faults` | 57,711 | Every invariant passes, 56 keys never answered |
| `no-idempotency` | `db-latency`, 8 s faults | 38,691 | Fails I4 (2,397), I5 (12), I6 (70), I7 (2,678) |
| `dual-write` | `kafka-down` | 8,229 | Fails I8 (925 transfers never on the topic) and I9 (20 accounts) |
| `unordered-locks` | `contention` | 1,670 | Fails I11 (112 deadlocks) |
| `read-modify-write` | `contention` | 3,681 | Fails I2 (20 accounts) and I11 (63 deadlocks) |

The key counts also show the cost of the naive locking: with the same 64 workers, `unordered-locks` and `read-modify-write` finished a small part of what the correct mode did, because every deadlock waits for `deadlock_timeout` before Postgres picks a victim.

False positives are caught by the correct mode, which must pass every invariant in every scenario.

### 5.13 Failure modes of the system

| Failure | Detection | Response | Compensation |
|---|---|---|---|
| Client timeout after commit | Client sees `unknown` | Retry with the same key, replay returns the result | (none needed) |
| Ledger killed mid transaction | Connection drop, Postgres rolls back | Client retries with the same key | (none needed) |
| Relay killed after send, before mark | Duplicate events on the topic | Projector dedups by `eventId` | (none needed) |
| Part of a relay batch acked, then one send fails | Send error in the relay | Whole batch rolls back, acked rows go out again with the same `eventId` | (none needed) |
| Event parses but Postgres refuses it | Projector logs the transfer id and offset on every retry | Partition stops, quiet period times out, I9 fails | Fix the projector, the record is still on the topic |
| Projector group starts after the first events | (prevented) | `auto.offset.reset=earliest` | (none needed) |
| Kafka down | Relay send fails within the producer timeouts | Transaction rolls back, outbox rows wait for the next poll | (none needed) |
| Projector killed after commit, before ack | Record replayed | `processed_event` skips it | (none needed) |
| Postgres paused | Client socket timeout, `503` | Client retries with the same key. Projector backs off without skipping | (none needed) |
| Bug in posting code | Deferred trigger fails the commit | `500`, transaction rolled back | Fix the bug, no data repair |

### 5.14 Results

`scripts/experiments.sh` runs every mode in its scenario for `DURATION` (60 s) and the locking comparison for `LOCK_DURATION` (180 s), 64 workers, the `.env` values otherwise. Keys are the workload keys, without the 20 funding transfers. The runs below are on a laptop, with the Java runner and checker. `results/sample` keeps their `report.json`, and the report page reads them.

The runner is closed loop, and it shares the Docker VM's 4 CPUs with Postgres, Kafka and the three services. The mode runs went while the Docker disk was filling, which slowed them down but did not change what the checker found. Their throughput is left out: the table shows what each mode broke.

| Mode | Locking | Scenario | Keys | Faults | Verdict |
|---|---|---|---|---|---|
| `correct` | ordered | `all` | 37,945 | 7 | Every invariant passes |
| `correct` | serializable | `all` | 23,615 | 7 | Every invariant passes |
| `correct` | ordered | `db-latency`, 8 s faults | 27,871 | 4 | Every invariant passes |
| `no-idempotency` | ordered | `db-latency`, 8 s faults | 28,894 | 4 | Fails I4 (1,857), I5 (6), I6 (49), I7 (1,991) |
| `dual-write` | ordered | `kafka-down` | 5,363 | 7 | Fails I8 (407), I9 (20) |
| `unordered-locks` | ordered | `contention` | 1,949 | 0 | Fails I11 (128 deadlocks) |
| `read-modify-write` | ordered | `contention` | 102,349 | 0 | Fails I2 (20), I3 (5) |

Locking under contention, `correct` mode, no faults, on a clean disk, 180 s each so the JIT warm-up of a fresh stack weighs less:

| Accounts | Locking | ok/s | p50 ms | p99 ms | Rollbacks | Unknown answers |
|---|---|---|---|---|---|---|
| 20 | ordered | 2,036 | 28 | 89 | 0 | 0 |
| 20 | serializable | 1,078 | 42 | 163 | 465,560 | 30,410 |
| 200 | ordered | 2,802 | 22 | 55 | 0 | 0 |
| 200 | serializable | 2,222 | 26 | 80 | 121,344 | 414 |

Ordered `for update` wins at both contention levels. With 20 accounts, serializable rolls back 2.4 transactions per ok answer and answers about half as many transfers per second. Its unknown answers are `503`s after `SERIALIZABLE_RETRIES` ran out, which the client retries with the same key, so the invariants still hold. With 200 accounts fewer transfers touch the same rows: 0.3 rollbacks per ok answer, and serializable reaches 79% of ordered. Ordered locks never roll back for contention: a transfer waits for the row instead of doing the work and throwing it away.

`read-modify-write` holds no locks while it reads, so it answers more transfers than the correct mode in the same 60 s. It is also wrong.

---

## 6. Scaffolding

```
ledger-chaos/
├── .env
├── .gitignore
├── CLAUDE.md                        local only, ignored globally
├── Makefile                         up, test, chaos, chaos-all, report, down, clean
├── compose.yaml                     postgres, db-init, kafka, kafka-topics, toxiproxy, later the services
├── infra/
│   ├── postgres/migrations/         Flyway, run by db-init: roles, databases, role timeouts
│   └── toxiproxy/toxiproxy.json     proxies postgres (15432) and kafka (19092)
├── sonar-project.properties
├── README.md
├── services/
│   ├── pom.xml                      aggregator: ledger, relay, projector
│   ├── mvnw
│   ├── Dockerfile                   one for all five modules, MODULE build arg. Target service: Temurin JRE. Target tool: jlink runtime on distroless
│   ├── ledger/
│   │   ├── pom.xml
│   │   └── src/
│   │       ├── main/java/dev/jaderss/ledger/
│   │       │   ├── Application.java
│   │       │   ├── api/
│   │       │   │   ├── CorrelationIdFilter.java     X-Request-Id into MDC and the response
│   │       │   │   ├── TransferController.java
│   │       │   │   ├── AccountController.java
│   │       │   │   ├── ProblemHandler.java          @RestControllerAdvice, ProblemDetail
│   │       │   │   └── TransferRequest.java, TransferResponse.java
│   │       │   ├── domain/                          no Spring, no JDBC
│   │       │   │   ├── Money.java                   record, minor units and currency
│   │       │   │   ├── AccountId.java, TransferId.java, IdempotencyKey.java   records, no raw UUID or String in signatures
│   │       │   │   ├── Account.java, Transfer.java, Posting.java
│   │       │   │   ├── TransferDecision.java        sealed: Post, Reject
│   │       │   │   ├── TransferCommand.java         parsed request, gives the RequestHash
│   │       │   │   ├── TransferPosted.java          event contract §5.7
│   │       │   │   └── RequestHash.java
│   │       │   ├── app/
│   │       │   │   ├── LedgerProperties.java        @ConfigurationProperties, @Validated, mode and locking enums
│   │       │   │   ├── TransferService.java         interface
│   │       │   │   ├── TransferOutcome.java         sealed: Created, Replayed, KeyMismatch
│   │       │   │   ├── TransferServiceConfig.java   one line per mode: which claim, load and publish steps
│   │       │   │   ├── TransferSteps.java           record of the three steps a mode can swap
│   │       │   │   ├── LedgerTransferService.java   the transaction of §5.6 over the steps
│   │       │   │   └── TransactionPolicy.java       isolation level and retry budget per locking strategy
│   │       │   ├── messaging/
│   │       │   │   └── DirectPublisher.java         dual-write only: send after commit, no outbox
│   │       │   └── persistence/                     Jdbi
│   │       │       ├── LedgerDatabase.java          the Jdbi instance, inTransaction and withStores
│   │       │       ├── DatabaseException.java       sealed: ConcurrencyConflict, IntegrityViolation, Unavailable
│   │       │       ├── Isolation.java               READ_COMMITTED, or SERIALIZABLE as the first statement
│   │       │       ├── AccountStore.java, IdempotencyStore.java, TransferStore.java, PostingStore.java, OutboxStore.java
│   │       │       └── LedgerStores.java            the five stores over one Handle
│   │       ├── main/resources/
│   │       │   ├── application.yaml
│   │       │   └── db/migration/V1__ledger.sql, V2__posting_triggers.sql, V3__relay_grants.sql
│   │       └── test/java/dev/jaderss/ledger/
│   │           ├── domain/MoneyTest.java, TransferDecisionTest.java, RequestHashTest.java
│   │           ├── domain/LedgerPropertyTest.java   jqwik
│   │           ├── app/TransferConcurrencyTest.java Testcontainers, virtual threads, parallel duplicate keys, lock races
│   │           ├── app/*ModeTest.java               one per naive mode, its defect without the chaos runner
│   │           ├── app/SerializableLockingTest.java contention, overspend and duplicate keys without row locks
│   │           ├── persistence/PostingTriggersTest.java   sum-zero and append-only triggers
│   │           ├── persistence/IsolationTest.java   the level each transaction really runs at
│   │           ├── api/TransferControllerTest.java  port 0, every row of §5.5 except 503
│   │           ├── api/AccountControllerTest.java
│   │           └── ArchitectureTest.java            ArchUnit, domain has no framework imports
│   ├── relay/
│   │   └── src/
│   │       ├── main/java/dev/jaderss/relay/
│   │       │   ├── Application.java
│   │       │   ├── OutboxRelay.java                 scheduled loop, one transaction per batch
│   │       │   ├── OutboxStore.java                 claim with SKIP LOCKED, mark published
│   │       │   ├── JdbiConfig.java                  the Jdbi bean, after Flyway
│   │       │   ├── RelayProperties.java             checks the timeout inequality of §5.8 at startup
│   │       │   └── ProducerSettings.java            acks=all, idempotence, producer timeouts
│   │       └── test/java/dev/jaderss/relay/
│   │           ├── OutboxRelayTest.java             crash between send and mark, parallel batches
│   │           └── RelayPropertiesTest.java
│   ├── projector/
│   │   └── src/
│   │       ├── main/java/dev/jaderss/projector/
│   │       │   ├── Application.java
│   │       │   ├── TransferPostedListener.java      @KafkaListener, manual ack
│   │       │   ├── KafkaErrorConfig.java            unlimited backoff, DLT only for records that do not parse
│   │       │   ├── BalanceProjection.java           processed_event and upserts in account id order, one Jdbi transaction
│   │       │   ├── JdbiConfig.java                  the Jdbi bean, after Flyway
│   │       │   └── TransferPosted.java              record, own copy of the contract, refuses unbalanced postings
│   │       ├── main/resources/db/migration/V1__projector.sql
│   │       └── test/java/dev/jaderss/projector/ProjectorTest.java   duplicates, parallel partitions, DLT
│   ├── checker/                     plain Java, no Spring
│   │   └── src/
│   │       ├── main/java/dev/jaderss/checker/
│   │       │   ├── Checker.java             quiet wait, load, check, print, report.json
│   │       │   ├── Config.java              settings from the environment
│   │       │   ├── Invariants.java          pure checks over a Snapshot
│   │       │   ├── Mode.java                Must fail set per mode, the verdict of the run
│   │       │   ├── Snapshot.java            history, ledger, topic, projector balances, quiet
│   │       │   ├── History.java             history.jsonl grouped by key
│   │       │   ├── Stats.java               throughput, latency, answers per second, fault windows
│   │       │   ├── Databases.java           Jdbi loaders for both databases
│   │       │   └── TopicReader.java         Kafka admin offsets and lag, an assigned consumer with no group
│   │       └── test/java/dev/jaderss/checker/
│   │           ├── InvariantsTest.java      one corruption per invariant
│   │           ├── StoresTest.java          loaders and topic reads, Testcontainers
│   │           ├── CheckerTest.java         whole run against a fixture system
│   │           └── ArchitectureTest.java    no service code, no Spring
│   └── chaos/                       plain Java, Jackson only
│       └── src/
│           ├── main/java/dev/jaderss/chaos/
│           │   ├── Chaos.java               heal, setup, funding, workload and nemesis until the deadline, run.json
│           │   ├── Config.java              settings from the environment
│           │   ├── GoDuration.java          durations like 60s and 1500ms, as the scripts pass them
│           │   ├── LedgerClient.java        java.net.http on HTTP/1.1
│           │   ├── Outcome.java             classifies answers into ok, fail, unknown
│           │   ├── Workload.java            workers on virtual threads, retries with the same key
│           │   ├── Scenario.java            the faults of each scenario
│           │   ├── Faults.java              fault catalog, heal all
│           │   ├── Nemesis.java             one fault per interval, healed after its duration
│           │   ├── Docker.java              Docker Engine API, HTTP/1.0 over the Unix socket
│           │   ├── Toxiproxy.java
│           │   └── JsonLog.java             history.jsonl and faults.jsonl
│           └── test/java/dev/jaderss/chaos/
│               ├── LedgerClientTest.java    WireMock: reset, empty response, slow answer, connection reuse
│               ├── WorkloadTest.java        retries, retry budget, funding, a short run
│               ├── FaultsTest.java          Docker over a fake daemon on a Unix socket, Toxiproxy, nemesis
│               ├── ChaosTest.java           whole run against fakes
│               └── ArchitectureTest.java    no service or checker code, no Spring
├── report/                          React, Vite, Recharts, nginx. Reads every results/sample/*/report.json at build time
├── scripts/
│   ├── chaos.sh                     one mode and scenario per call
│   └── experiments.sh               every mode in its scenario, and the locking comparison
├── results/
│   └── sample/
├── docs/
│   ├── PLAN.md
│   ├── SDD.md
│   └── INVARIANTS.md                written in Phase 7
└── .github/workflows/
    ├── ci.yml                       build, tests, sonar
    └── chaos.yml                    short chaos per mode, on push to main and nightly
```

---

## 7. Libraries and versions

Checked on Maven Central, Docker Hub and GitHub releases on 2026-11-04. JVM libraries marked **BOM** come from `spring-boot-dependencies` 4.1.2 and are not overridden in the poms.

### 7.1 Runtime

| Item | Version | Notes |
|---|---|---|
| Java (Temurin) | 25 | LTS. The services run on the Temurin JRE image |
| Tool runtime | `jlink` from Temurin 25 on `gcr.io/distroless/cc-debian13:nonroot` | `chaos` and `checker`: only the JDK modules `jdeps` finds in them and their libraries, 63 MB instead of 191 MB for the JRE, shared by both images. `java.desktop` stays because Jackson reaches `java.beans` |
| Spring Boot | 4.1.2 | Parent of the three services |
| Spring Framework | 7.0.10 | BOM |
| Spring Kafka | 4.1.2 | BOM, `relay` and `projector`, and `ledger` for the `dual-write` mode only |
| Kafka clients | 4.2.2 | BOM, also the checker's admin client and consumer |
| Jackson | 3.1.7 | BOM |
| Flyway | 12.4.0 | BOM, plus `flyway-database-postgresql` |
| PostgreSQL JDBC | 42.7.14 | BOM |
| HikariCP | 7.0.2 | BOM |
| Jdbi | 3.55.0 | `jdbi3-core` and `jdbi3-postgres` in the three services and the checker. Not in the BOM: `jdbi3-bom` is imported in the parent pom |
| Micrometer | 1.17.2 | BOM, Prometheus registry through Actuator |
| PostgreSQL image | 18.6 | One instance, two databases |
| Kafka image | `apache/kafka:4.4.0` | KRaft, single broker |
| Toxiproxy image | `ghcr.io/shopify/toxiproxy:2.13.0` | |
| Flyway image | `flyway/flyway:12.4.0` | `db-init`, same version as the BOM |
| Node, nginx | 26, 1.31 | `report`, same as prime-bench |

### 7.2 Build and test

| Item | Version | Notes |
|---|---|---|
| JUnit | 6.0.3 | BOM |
| AssertJ | 3.27.7 | BOM |
| Testcontainers | 2.0.5 | BOM, modules `testcontainers-postgresql`, `testcontainers-kafka`, `testcontainers-toxiproxy` |
| Awaitility | 4.3.0 | BOM |
| WireMock | 3.13.2 | `wiremock-standalone`, `chaos` tests only: connection resets, empty responses, slow answers. Not in the BOM, pinned in the parent pom |
| jqwik | 1.10.2 | Property tests, not in the BOM |
| ArchUnit | 1.5.1 | `archunit-junit5`, domain package rule |
| PIT | 1.30.1 | `pitest-maven` with `pitest-junit5-plugin` 1.2.3, domain package only |
| Spotless Maven plugin | 3.11.0 | Palantir format |
| Palantir Java Format | 2.103.0 | |
| JaCoCo | 0.8.16 | XML for Sonar |

### 7.3 Tools

| Tool | Version | Notes |
|---|---|---|
| Chaos Mesh | 2.8.7 | Optional Kubernetes run |
| GitHub Actions | `checkout@v7`, `setup-java@v6`, `setup-node@v7` | |

---

## 8. Configuration

`.env` starting values. Workload numbers are a first guess, set after the first runs.

```bash
JAVA_VERSION=25
GO_VERSION=1.27
POSTGRES_VERSION=18.6
KAFKA_VERSION=4.4.0
TOXIPROXY_VERSION=2.13.0
FLYWAY_VERSION=12.4.0
NODE_VERSION=26
NGINX_VERSION=1.31
REPORT_PORT=8090

APP_CPUS=2
APP_MEMORY_MB=1536
HEAP_PERCENT=75
JVM_GC=ParallelGC

LEDGER_MODE=correct
LEDGER_LOCKING=ordered
SERIALIZABLE_RETRIES=5
STATEMENT_TIMEOUT=2s
DB_SOCKET_TIMEOUT_S=5
POOL_SIZE=20

RELAY_BATCH=100
RELAY_INTERVAL=50ms
IDLE_IN_TX_TIMEOUT=10s
KAFKA_DELIVERY_TIMEOUT_MS=5000
KAFKA_REQUEST_TIMEOUT_MS=3000
KAFKA_MAX_BLOCK_MS=3000
TOPIC_PARTITIONS=6
PROJECTOR_CONCURRENCY=3
KAFKA_SESSION_TIMEOUT_MS=10000

ACCOUNTS=20
WORKERS=64
READERS=4
DURATION=120s
CLIENT_TIMEOUT=1s
RETRIES=5
DUPLICATE_RATE=0.05
MISMATCH_RATE=0.01
FAULT_INTERVAL=10s
FAULT_DURATION=3s
QUIET_TIMEOUT=60s
FUNDING_MINOR=100000
AMOUNT_MAX_MINOR=5000
OVERDRAFT_EVERY=10
DB_LATENCY=1500ms
SEED=
```

Kafka sits behind Toxiproxy, so its advertised listener points at the proxy address. Without that, clients reconnect around the proxy after the first metadata call. The broker has four listeners: `CLIENT` on 9092, advertised as `toxiproxy:19092`, for the services; `INTERNAL` on 29092 for the broker itself and topic setup, so a cut proxy never takes the broker down; `CONTROLLER` on 9093; `HOST` on 9094, advertised as `localhost:9094`, for tools on the laptop.

Postgres has one role per service. The `db-init` service runs Flyway as `postgres` against the `postgres` database: `V1` creates the roles, `V2` creates the `ledger` and `projector` databases outside a transaction, and the repeatable `R__role_settings` sets `statement_timeout` on all three roles and `idle_in_transaction_session_timeout` on `relay` (§5.8), so the limits hold whatever the client does. Passwords and timeouts come from `.env` as placeholders. A changed timeout changes the script's checksum, so the next `make up` applies it without recreating Postgres. Each service still runs its own schema migrations with Flyway inside Spring Boot. The checker connects as `postgres`, straight to port 5432, because it needs `pg_stat_reset()` and must never go through a fault.

---

## 9. Testing and CI

| Level | What | Where |
|---|---|---|
| Unit | `Money`, `TransferDecision`, request hash | `ledger` domain |
| Property | Random transfer sequences keep I1 to I3 | `LedgerPropertyTest` (jqwik) |
| Mutation | PIT on the domain package | `./mvnw -Pmutation verify`, not on every push |
| Integration | Postgres, Kafka, Toxiproxy in Testcontainers. Duplicates, parallel keys, relay crash, projector crash | Each service |
| HTTP | Server on port 0, every row of the §5.5 table | `TransferControllerTest` |
| Architecture | Domain has no Spring or JDBC imports | `ArchitectureTest` |
| Chaos | Short run per mode and scenario | `chaos.yml` |

`chaos.yml` runs a matrix of modes, each with the scenario from §5.12, plus `correct` on `all` with each locking strategy and on `db-latency` with the long fault, the only run where I6 has keys to check in the correct mode. Every job passes when the checker's verdict passes (§5.11). A naive mode that stops failing means the checker or the scenario lost its teeth.

---

## 10. Alternatives considered

### 10.1 Debezium CDC instead of the polling relay

**What:** Debezium reads the Postgres WAL and publishes outbox rows.
**Why not now:** adds Kafka Connect and logical replication setup to a project whose point is the invariants. Kept as a later mode, since the checker does not change.

### 10.2 `JdbcClient`, jOOQ or Spring Data JDBC instead of Jdbi

**What:** Spring's `JdbcClient` with `TransactionTemplate`, which the services used up to Phase 5, or typed SQL, or repositories.
**Why not:** Jdbi keeps the SQL as plain text blocks, so locking and `on conflict` stay visible, and the transaction is explicit in the code: `inTransaction` with its isolation level, `afterCommit` on the handle. With Spring the transaction is bound to the thread, and the error types come from Spring's translation instead of the SQLState. jOOQ adds code generation and 5 MB for a few statements, Spring Data hides them behind repositories. `jdbi3-spring` would join Spring's transactions, and `jdbi3-sqlobject` would move the SQL into annotations: both rejected, one transaction model and visible SQL. `spring-jdbc` stays on the classpath, because Boot's `DataSource` and Flyway setup need it, and `ArchitectureTest` keeps the code off it.

### 10.3 Jepsen and Elle instead of an own checker

**What:** the reference tools for this kind of test.
**Why not:** Clojure and a separate control node raise the bar for readers and contributors. The checker covers the ledger rules in the same language as the services, and linearizability (I10) stays a stretch goal.

### 10.4 `BigDecimal` for money

**What:** the usual Java choice.
**Why not:** amounts are whole minor units, `long` with `Math.addExact` is exact and maps to `bigint` in Postgres. Same choice as the prime-bench gateway contract.

### 10.5 Hexagonal ports and adapters for the ledger

**What:** `app` owns interfaces such as `Accounts` and `Outbox`, and `persistence` implements them, so dependencies point inward.
**Why not:** every port would have one adapter, Postgres. The usual payoff is testing `app` with in-memory fakes, but what this project proves happens inside Postgres: the wait on the idempotency primary key, `for update` in id order, the deferred trigger. A fake would pass where the database fails, so `app` is tested against Postgres in Testcontainers. The domain is already free of framework code (§5.2), which is the part of hexagonal that matters here.

---

## 11. Risks and mitigations

| Risk | Likelihood | Impact | Mitigation |
|---|---|---|---|
| Checker has a bug and reports green | Medium | High | Naive modes must fail. Hand-made corruption tests per invariant. |
| Relay holds a database transaction while waiting for Kafka acks | High | Medium | Small batches, producer timeouts below `idle_in_transaction_session_timeout` (§5.8). Lease column as a later option. |
| Kafka through Toxiproxy misbehaves (advertised listeners) | Medium | Medium | Configure the listener to the proxy. Fall back to Docker stop and start only. |
| Chaos runs flaky in CI | Medium | Medium | Fixed seed per run, short duration in CI, long runs on the laptop. |
| Readers take the project as a production ledger | Low | Medium | README says what it is and what it skips (§4). |

---

## 12. Open questions

| # | Question | Why it matters | Default assumed in this doc |
|---|---|---|---|
| Q1 | Debezium mode in v1? | Common in production. | No, later mode. |
| Q2 | Multi-currency with FX? | Realism against size. | No. |
| Q3 | Postgres replica and failover? | Tests lost commits after promotion, a real finding. | No, single node. |
| Q4 | Linearizability check (I10) in v1? Porcupine is Go only, so it needs a JVM checker or a small one of our own. | Strong result, more work. | Stretch goal. |
| Q5 | Number of accounts for contention? | Too few and every transfer waits, too many and nothing collides. | 20, adjusted after the first runs. |
| Q6 | `FUNDING_MINOR` per account? | Too high and nothing is ever rejected, too low and most transfers fail. It also decides whether `read-modify-write` breaks I3. | 100000 per account with amounts up to 5000, a first guess. The first 30 s contention run had 759 insufficient-funds rejections against 51,594 ok answers. `read-modify-write` did not break I3 in its first 60 s contention run, which deadlocked most of the time. Without the deadlocks it sent 133,417 transfers in 60 s and broke I3 on 5 accounts (§5.14). |

---

## 13. References

* RFC 9457, Problem Details for HTTP APIs.
* IETF draft `draft-ietf-httpapi-idempotency-key-header`.
* Chris Richardson, Transactional Outbox pattern.
* Kyle Kingsbury, Jepsen analyses, for the history and nemesis model.
* Porcupine, linearizability checker in Go.
