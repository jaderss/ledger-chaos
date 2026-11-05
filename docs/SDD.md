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
| `report` | Results page per mode and scenario | `report` |

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
| Database unavailable or timeout | `503` | Problem `unavailable`. Outcome unknown to the client |

Errors use `ProblemDetail` (RFC 9457), with `type` under `/problems/`.

`X-Request-Id` is optional. `CorrelationIdFilter` generates one when it is missing, puts it in MDC, and returns it in the response header. The ledger stores it on the transfer and in the event, and the relay and projector log it, so one id follows a transfer from the request to the read model.

Other endpoints: `GET /transfers/{id}`, `GET /transfers?idempotencyKey=` (used by the checker for unknown outcomes), `GET /accounts/{id}`, `POST /accounts` (setup only).

`POST /accounts` takes an id, a currency and `allowOverdraft`, never an opening balance. A balance that does not come from postings would break I2. Money enters through one **mint** account per currency, created with `allowOverdraft = true`. Before the faults start, the chaos runner sends one funding transfer of `FUNDING_MINOR` from the mint to each workload account, through `POST /transfers` and recorded in the history like any other call. The mint goes negative by the total funded, so I1 still holds, and the projector sees the funding events like any other. The workload never uses the mint after that.

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

Timeouts: `STATEMENT_TIMEOUT` on the server, `DB_SOCKET_TIMEOUT_S` (pgjdbc `socketTimeout`) on the client. When Postgres is paused, the server enforces nothing, and only the client-side timeout ends the call with a `503`.

`LEDGER_LOCKING=serializable` replaces step 4 with `SERIALIZABLE` isolation and no row locks. The retry wraps the whole transaction: a `TransactionTemplate` inside a loop catches `ConcurrencyFailureException` (Spring's translation of SQLState `40001`) up to `SERIALIZABLE_RETRIES` times. A catch inside a `@Transactional` method would retry inside a transaction that is already rolled back.

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

A crash after the send and before the commit republishes the batch. That is expected and covered by the projector. `SKIP LOCKED` lets more than one relay run without double claiming a row.

The relay holds the transaction while it waits for acks, so a Kafka outage must not keep it open. Producer `delivery.timeout.ms`, `request.timeout.ms` and `max.block.ms` stay below `idle_in_transaction_session_timeout`, set on the relay's database role. When Kafka is down the send fails inside that limit, the transaction rolls back, the rows stay unpublished, and the next poll tries again.

### 5.9 Projector loop

Per record, one transaction:

1. `insert into processed_event (event_id) values (:id) on conflict do nothing`.
2. Inserted: upsert each posting into `balance` (`balance_minor = balance.balance_minor + excluded.balance_minor`).
3. Commit, then acknowledge the offset (`AckMode.MANUAL_IMMEDIATE`).

A crash after the commit and before the ack replays the record, and step 1 skips it.

Error handling: the default Spring Kafka `DefaultErrorHandler` retries 9 times, then commits the offset and the event is gone. The projector sets a `DefaultErrorHandler` with an unlimited `ExponentialBackOff` for database errors, so a paused database stops consumption instead of skipping records. Only a record that cannot be parsed goes to `ledger.transfers.DLT`. The checker reads the DLT, and any record there fails I9.

### 5.10 Chaos runner

Workload:

* `ACCOUNTS` accounts in one currency, a few with overdraft. A small number on purpose, to force lock contention.
* `WORKERS` goroutines send transfers with random pairs and amounts, each with a new UUID key.
* Timeout, connection error or `5xx`: record `unknown`, retry with the same key up to `RETRIES` times.
* `DUPLICATE_RATE` of transfers are sent twice in parallel with the same key.
* `MISMATCH_RATE` of transfers reuse a key with another body. Only keys that already got a definite `201` or `200` are reused. A key whose outcome is still unknown may never have committed, and then the mismatch body would become the transfer in a correct ledger and fail I7 for the wrong reason.
* `READERS` goroutines read balances for I10.

History, one JSON line per event:

```json
{"t":1730744525114,"worker":7,"op":"transfer","key":"7f1c...","phase":"invoke","body":{"from":"0b5f...","to":"9c21...","amount":2500}}
{"t":1730744525161,"worker":7,"op":"transfer","key":"7f1c...","phase":"ok","status":201,"transferId":"e2b9..."}
{"t":1730744526002,"worker":3,"op":"transfer","key":"a90d...","phase":"unknown","error":"context deadline exceeded"}
```

Nemesis: every `FAULT_INTERVAL` it picks a fault from the scenario, applies it for `FAULT_DURATION`, heals it, and writes `faults.jsonl`.

| Fault | How |
|---|---|
| `kill-ledger` | Docker API `POST /containers/{id}/kill` with `SIGKILL`, then start |
| `kill-relay` | Same, on `relay` |
| `kill-projector` | Same, on `projector` |
| `pause-db` | Docker API `pause` and `unpause` on Postgres |
| `restart-db` | Docker API `restart` on Postgres |
| `db-cut` | Toxiproxy: disable the Postgres proxy |
| `db-latency` | Toxiproxy `latency` toxic above the client timeout |
| `db-reset` | Toxiproxy `reset_peer` toxic |
| `kafka-down` | Docker API `stop` and `start` on Kafka |

Docker Engine API through the Unix socket with `net/http`, no Docker SDK.

A **scenario** is a named set of faults for the nemesis. `make chaos MODE=... SCENARIO=...` runs one pair.

| Scenario | Faults | Why it exists |
|---|---|---|
| `contention` | (none) | Lock contention only. Breaks `read-modify-write` and `unordered-locks`, and is the baseline for the correct mode |
| `db-latency` | `db-latency` | Client timeouts on transfers that still commit, so retries hit keys with unknown outcome. Breaks `no-idempotency` on I6 |
| `kafka-down` | `kafka-down` | Breaks `dual-write`: the transfer commits and the publish fails |
| `kills` | `kill-ledger`, `kill-relay`, `kill-projector` | Crashes between the steps of §5.6, §5.8 and §5.9 |
| `db-faults` | `pause-db`, `restart-db`, `db-cut`, `db-reset` | Database gone or slow in every way the catalog has |
| `all` | Every fault above | Long runs on the laptop and the correct mode in CI |

End of run: heal everything, stop the workload, wait until `outbox` has no unpublished rows and the projector consumer group has no lag, with `QUIET_TIMEOUT` as the limit. A timeout is reported as an I8 or I9 failure.

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
| I8 | Every `POSTED` transfer is on the topic at least once, matched by `transferId`. In `correct` mode also: one outbox row per `POSTED` transfer and none unpublished |
| I9 | Projector `balance` equals the ledger's sum of postings for every account, and `ledger.transfers.DLT` is empty. An account with no projector row counts as zero. The sum of postings, not `account.balance_minor`, so a wrong stored balance fails I2 only |
| I10 | Porcupine model of a register per account over transfers and reads (stretch goal) |
| I11 | `pg_stat_database.deadlocks` for the ledger database is zero. `scripts/chaos.sh` calls `pg_stat_reset()` on that database before the workload |

Output: one line per invariant with pass or fail and a count, plus `report.json`. Exit code `1` on any failure.

### 5.12 Naive modes

`LEDGER_MODE` selects the `TransferService` bean. One class per mode, small and obviously wrong. Never on by default. `LEDGER_MODE` and `LEDGER_LOCKING` bind to enums in a `LedgerProperties` record with `@ConfigurationProperties` and `@Validated`, so a typo fails startup instead of running the default mode.

| Mode | Change | Scenario | Must fail |
|---|---|---|---|
| `correct` | Algorithm in §5.6 | Every scenario | (none) |
| `no-idempotency` | Skips steps 2 and 3. A retry with the same key posts again, with its own `transfer` row and the same `request_key` | `db-latency` | I4, I6, I7 |
| `dual-write` | No outbox row. Publishes to Kafka after the commit from the ledger | `kafka-down` | I8, I9 |
| `unordered-locks` | Locks `from` then `to` | `contention` | I11 |
| `read-modify-write` | Reads balances without `for update`, writes computed values | `contention` | I2 |

A naive run passes when the checker fails at least its **Must fail** set. Other failures go in the report but do not fail the job: `read-modify-write` also fails I3 when a lost update lets an account without overdraft overspend, and that depends on `FUNDING_MINOR` against the transfer amounts. False positives are caught by the correct mode, which must pass every invariant in every scenario.

### 5.13 Failure modes of the system

| Failure | Detection | Response | Compensation |
|---|---|---|---|
| Client timeout after commit | Client sees `unknown` | Retry with the same key, replay returns the result | (none needed) |
| Ledger killed mid transaction | Connection drop, Postgres rolls back | Client retries with the same key | (none needed) |
| Relay killed after send, before mark | Duplicate events on the topic | Projector dedups by `eventId` | (none needed) |
| Kafka down | Relay send fails within the producer timeouts | Transaction rolls back, outbox rows wait for the next poll | (none needed) |
| Projector killed after commit, before ack | Record replayed | `processed_event` skips it | (none needed) |
| Postgres paused | Client socket timeout, `503` | Client retries with the same key. Projector backs off without skipping | (none needed) |
| Bug in posting code | Deferred trigger fails the commit | `500`, transaction rolled back | Fix the bug, no data repair |

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
│   ├── ledger/
│   │   ├── Dockerfile
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
│   │       │   │   └── RequestHash.java
│   │       │   ├── app/
│   │       │   │   ├── LedgerProperties.java        @ConfigurationProperties, @Validated, mode and locking enums
│   │       │   │   ├── TransferService.java         interface
│   │       │   │   ├── LockingTransferService.java  correct mode
│   │       │   │   ├── SerializableTransferService.java
│   │       │   │   └── naive/                       one class per naive mode
│   │       │   └── persistence/                     JdbcClient stores
│   │       │       ├── AccountStore.java, TransferStore.java, PostingStore.java, OutboxStore.java
│   │       ├── main/resources/
│   │       │   ├── application.yaml
│   │       │   └── db/migration/V1__ledger.sql, V2__posting_triggers.sql
│   │       └── test/java/dev/jaderss/ledger/
│   │           ├── domain/MoneyTest.java, TransferDecisionTest.java
│   │           ├── domain/LedgerProperties.java     jqwik
│   │           ├── app/TransferConcurrencyTest.java Testcontainers, virtual threads
│   │           ├── app/IdempotencyTest.java
│   │           ├── api/TransferControllerTest.java  port 0
│   │           └── ArchitectureTest.java            ArchUnit, domain has no framework imports
│   ├── relay/
│   │   └── src/main/java/dev/jaderss/relay/
│   │       ├── Application.java
│   │       ├── OutboxRelay.java                     scheduled loop
│   │       └── OutboxStore.java
│   └── projector/
│       └── src/main/java/dev/jaderss/projector/
│           ├── Application.java
│           ├── TransferPostedListener.java          @KafkaListener, manual ack
│           ├── KafkaErrorConfig.java                unlimited backoff, DLT only for parse errors
│           ├── BalanceProjection.java               processed_event and upsert
│           └── TransferPosted.java                  record, own copy of the contract
├── chaos/                           Go
│   ├── go.mod
│   ├── main.go
│   ├── workload.go
│   ├── nemesis.go
│   ├── docker.go                    Docker Engine API over the Unix socket
│   ├── toxiproxy.go
│   ├── history.go
│   └── *_test.go
├── checker/                         Go
│   ├── go.mod
│   ├── main.go
│   ├── invariants.go
│   ├── history.go
│   ├── topic.go                     franz-go, reads ledger.transfers from the start
│   ├── linearizability.go           Porcupine, stretch goal
│   └── *_test.go
├── report/                          React, Vite, Recharts, nginx
├── scripts/
│   └── chaos.sh                     one mode and scenario per call
├── results/
│   └── sample/
├── docs/
│   ├── PLAN.md
│   ├── SDD.md
│   └── INVARIANTS.md                written in Phase 6
└── .github/workflows/
    ├── ci.yml                       build, tests, sonar
    └── chaos.yml                    short chaos per mode, on push to main and nightly
```

---

## 7. Libraries and versions

Checked on Maven Central, the Go module proxy, Docker Hub and GitHub releases on 2026-11-04. JVM libraries marked **BOM** come from `spring-boot-dependencies` 4.1.2 and are not overridden in the poms.

### 7.1 Runtime

| Item | Version | Notes |
|---|---|---|
| Java (Temurin) | 25 | LTS |
| Spring Boot | 4.1.2 | Parent of the three services |
| Spring Framework | 7.0.10 | BOM |
| Spring Kafka | 4.1.2 | BOM, `relay` and `projector` |
| Kafka clients | 4.2.2 | BOM |
| Jackson | 3.1.7 | BOM |
| Flyway | 12.4.0 | BOM, plus `flyway-database-postgresql` |
| PostgreSQL JDBC | 42.7.14 | BOM |
| HikariCP | 7.0.2 | BOM |
| Micrometer | 1.17.2 | BOM, Prometheus registry through Actuator |
| Go | 1.27 | `chaos` and `checker` |
| pgx | v5.12.0 | `checker` |
| franz-go | v1.22.2 | `checker`, topic scan |
| Toxiproxy Go client | v2.13.0 | `chaos` |
| Porcupine | v1.3.1 | `checker`, stretch goal |
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
| GitHub Actions | `checkout@v7`, `setup-java@v6`, `setup-go`, `setup-node@v7` | |

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
FUNDING_MINOR=
SEED=
```

Kafka sits behind Toxiproxy, so its advertised listener points at the proxy address. Without that, clients reconnect around the proxy after the first metadata call. The broker has four listeners: `CLIENT` on 9092, advertised as `toxiproxy:19092`, for the services; `INTERNAL` on 29092 for the broker itself and topic setup, so a cut proxy never takes the broker down; `CONTROLLER` on 9093; `HOST` on 9094, advertised as `localhost:9094`, for tools on the laptop.

Postgres has one role per service. The `db-init` service runs Flyway as `postgres` against the `postgres` database: `V1` creates the roles, `V2` creates the `ledger` and `projector` databases outside a transaction, and the repeatable `R__role_settings` sets `statement_timeout` on all three roles and `idle_in_transaction_session_timeout` on `relay` (§5.8), so the limits hold whatever the client does. Passwords and timeouts come from `.env` as placeholders. A changed timeout changes the script's checksum, so the next `make up` applies it without recreating Postgres. Each service still runs its own schema migrations with Flyway inside Spring Boot. The checker connects as `postgres`, straight to port 5432, because it needs `pg_stat_reset()` and must never go through a fault.

---

## 9. Testing and CI

| Level | What | Where |
|---|---|---|
| Unit | `Money`, `TransferDecision`, request hash | `ledger` domain |
| Property | Random transfer sequences keep I1 to I3 | `LedgerProperties` (jqwik) |
| Mutation | PIT on the domain package | `./mvnw -Pmutation verify`, not on every push |
| Integration | Postgres, Kafka, Toxiproxy in Testcontainers. Duplicates, parallel keys, relay crash, projector crash | Each service |
| HTTP | Server on port 0, every row of the §5.5 table | `TransferControllerTest` |
| Architecture | Domain has no Spring or JDBC imports | `ArchitectureTest` |
| Chaos | Short run per mode and scenario | `chaos.yml` |

`chaos.yml` runs a matrix of modes, each with the scenario from §5.12. The `correct` job runs `all` and passes when the checker exits `0`. Each naive job passes when the checker exits `1` and the report names at least its **Must fail** invariants. A naive mode that stops failing means the checker or the scenario lost its teeth.

---

## 10. Alternatives considered

### 10.1 Debezium CDC instead of the polling relay

**What:** Debezium reads the Postgres WAL and publishes outbox rows.
**Why not now:** adds Kafka Connect and logical replication setup to a project whose point is the invariants. Kept as a later mode, since the checker does not change.

### 10.2 jOOQ or Spring Data JDBC instead of `JdbcClient`

**What:** typed SQL or repositories.
**Why not:** a few statements, all visible in one place. Locking and `on conflict` are clearer as plain SQL.

### 10.3 Jepsen and Elle instead of a Go checker

**What:** the reference tools for this kind of test.
**Why not:** Clojure and a separate control node raise the bar for readers and contributors. The Go checker covers the ledger rules, and Porcupine covers linearizability.

### 10.4 `BigDecimal` for money

**What:** the usual Java choice.
**Why not:** amounts are whole minor units, `long` with `Math.addExact` is exact and maps to `bigint` in Postgres and in Go. Same choice as the prime-bench gateway contract.

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
| Q4 | Porcupine check in v1? | Strong result, more work. | Stretch goal. |
| Q5 | Number of accounts for contention? | Too few and every transfer waits, too many and nothing collides. | 20, adjusted after the first runs. |
| Q6 | `FUNDING_MINOR` per account? | Too high and nothing is ever rejected, too low and most transfers fail. It also decides whether `read-modify-write` breaks I3. | Unset, picked after the first runs. |

---

## 13. References

* RFC 9457, Problem Details for HTTP APIs.
* IETF draft `draft-ietf-httpapi-idempotency-key-header`.
* Chris Richardson, Transactional Outbox pattern.
* Kyle Kingsbury, Jepsen analyses, for the history and nemesis model.
* Porcupine, linearizability checker in Go.
