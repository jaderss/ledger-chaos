# ledger-chaos: plan

> **Status:** Draft.
> **Companion:** [`SDD.md`](SDD.md) has the design, the scaffolding and the versions.
> **Open questions:** see [§8](#8-open-questions).

---

## 1. TL;DR

A double-entry ledger that moves money between accounts, with idempotency keys, a transactional outbox and an idempotent consumer. A chaos runner sends transfers while it kills processes, cuts the network and slows the database. After each run a checker proves that no cent was lost, created or applied twice.

The same chaos also runs against naive versions of the ledger (dual write, no idempotency key, unordered locks). The checker must catch each one. That proves the checker works, and the numbers from the naive versions make the headline.

---

## 2. Invariants

The project is about these rules. Every phase adds a way to break them and a way to check them.

| # | Invariant | Checked by |
|---|---|---|
| I1 | The sum of all postings per currency is zero. | Checker, SQL |
| I2 | Each account balance equals the sum of its postings. | Checker, SQL |
| I3 | No account without overdraft goes below zero. | Database constraint on the stored balance, checker on the sum of postings |
| I4 | A transfer acknowledged to the client exists exactly once. | Checker against the client history |
| I5 | A transfer refused with a definite error does not exist. | Checker against the client history |
| I6 | A transfer with unknown outcome (timeout, reset) exists at most once. | Checker, lookup by idempotency key |
| I7 | Same key and same body give the same answer. Same key and another body give `422` and change nothing. | Checker against the client history |
| I8 | Every posted transfer reaches the topic once the system is quiet. In the correct mode it goes through exactly one outbox row. | Checker, SQL and Kafka |
| I9 | Projector balances equal the ledger's sums of postings once the system is quiet. | Checker |
| I10 | Reads of an account balance are linearizable. | Porcupine over the history (stretch goal) |
| I11 | No transfer transaction ends in a deadlock. | Checker, `pg_stat_database` |

---

## 3. Parts

| Part | Stack | Role |
|---|---|---|
| `ledger` | Spring Boot MVC, Java, virtual threads, `JdbcClient`, Flyway | Accepts transfers, writes postings and the outbox row in one transaction |
| `relay` | Spring Boot, Spring Kafka | Polls the outbox with `FOR UPDATE SKIP LOCKED`, publishes to Kafka, marks rows published |
| `projector` | Spring Boot, Spring Kafka | Consumes events, keeps its own balance table, dedups by event id in the same transaction |
| `chaos` | Java 25, no Spring | Workload, fault injection, history recording |
| `checker` | Java 25, no Spring | Reads the history, Postgres and Kafka, reports each invariant |

`relay` and `projector` are separate deployables so the chaos runner can kill them on their own.

---

## 4. Phases

No dates. Each phase ends with a check that says it is done.

### Phase 0: Bootstrap

* Repo files: `.gitignore`, `.env`, `Makefile`, `compose.yaml`.
* Postgres 18, Kafka 4 in KRaft mode, Toxiproxy in front of both.
* CI skeleton, SonarQube Cloud project.

Done when: `make up` starts the infrastructure and CI is green.

### Phase 1: Domain and ledger

* `Money` as integer minor units plus currency, `Math.addExact` for every sum.
* Schema with Flyway: `account`, `transfer`, `idempotency`, `posting`, `outbox`. A deferred constraint trigger rejects a transfer whose postings do not sum to zero, and a second trigger blocks updates and deletes on `posting`.
* Typed ids (`AccountId`, `TransferId`, `IdempotencyKey`) and a correlation id from `X-Request-Id`, stored on the transfer and carried in the event.
* `POST /transfers` with the `Idempotency-Key` header, errors as Problem Details (RFC 9457).
* Property tests with jqwik: random transfer sequences keep I1 to I3.
* Concurrency tests: many virtual threads on the same two accounts, duplicate keys sent in parallel.

Done when: unit, property and Testcontainers tests pass and Sonar shows 80% coverage on new code.

### Phase 2: Outbox and projector

* `relay` with an idempotent Kafka producer, `acks=all`.
* `projector` with a `processed_event` table and manual offset commit after the database commit.
* Testcontainers tests that kill the relay between publish and mark, and check the projector still ends equal.

Done when: I8 and I9 hold in integration tests with forced duplicates.

### Phase 3: Chaos runner and checker

* `chaos` sends transfers with idempotency keys, retries timeouts with the same key, records every call as invoke, ok, fail or unknown.
* Faults through the Docker Engine API (kill, stop, pause) and the Toxiproxy API (latency, reset, cut).
* `checker` reads the history and the final state, prints one line per invariant, exits non-zero on any violation.
* Scenarios: named sets of faults, listed in [SDD §5.10](SDD.md#510-chaos-runner).
* `make chaos MODE=... SCENARIO=...` runs funding, workload, faults, quiet period, checker.

Done when: a clean run passes every invariant and a hand-made corruption in the database fails the right one.

### Phase 4: Naive variants

Same API, broken on purpose, selected by `LEDGER_MODE`:

| Mode | What is wrong | Invariant that should break |
|---|---|---|
| `no-idempotency` | Skips the `idempotency` table, so a retry with the same key posts again | I4, I6, I7 |
| `dual-write` | Commits the transfer, then publishes to Kafka directly, no outbox | I8, I9 |
| `unordered-locks` | Locks accounts in request order | I11. Deadlocks, not lost money. Shows why ordering matters |
| `read-modify-write` | Reads the balance, computes in Java, writes it back without a lock | I2, and I3 when a lost update lets an account overspend |

Done when: each naive mode fails at least its expected invariants in its scenario ([SDD §5.12](SDD.md#512-naive-modes)), and the correct mode passes all of them in every scenario.

### Phase 5: Experiments and report

* Locking strategies on the correct ledger: ordered `SELECT ... FOR UPDATE` against `SERIALIZABLE` with retry. Throughput, p99 and retry rate under contention.
* Report page: fault timeline, client outcomes over time, invariant results per mode. Charts checked with the `dataviz` skill.
* Optional: Kubernetes run with Chaos Mesh.

Done when: each mode has a result row with transfers sent, faults injected, violations found.

### Phase 6: One language, Jdbi for every database call

The chaos runner and the checker move from Go to Java 25, and every database call moves from Spring's `JdbcClient` and `TransactionTemplate` to Jdbi. Go was never a decision with a reason: the only Go-only library, Porcupine, is not used yet, and a second language costs a toolchain in containers, two coverage setups and a second idiom for readers.

Stack:

* `chaos` and `checker` are plain Java modules under `services/`, no Spring context. Workers on virtual threads, `java.net.http.HttpClient` on HTTP/1.1, the Docker Engine API over `UnixDomainSocketAddress`.
* Jdbi 3.55.0 (`jdbi3-core`, `jdbi3-postgres`) in the three services and the checker. Jdbi owns the transactions: `inTransaction` with the isolation level, `afterCommit` on the handle, retries on SQLState `40001` and `40P01`.
* WireMock 3.13.2, test scope, for resets, empty responses and delays in the runner's client tests.
* ArchUnit: no `org.springframework.jdbc`, `dao` or `transaction` imports anywhere, and the checker imports no service code and no Spring.

Library decisions, with the criterion: real value, active, secure, small footprint, less boilerplate.

| Library | Decision | Why |
|---|---|---|
| Jdbi 3.55.0 (2026-09-27) | Adopt | One fluent chain per query, rows to records, explicit transactions. 1.1 MB, two small dependencies |
| WireMock 3.13.2 (2025-11-14) | Adopt, tests only | Built-in connection resets and empty responses, the cases the runner must classify as unknown |
| Spring Boot for the tools | Reject | Two databases in the checker undo most of what it saves. A config record replaces it |
| `docker-java-core` 3.7.1 | Reject | Jackson 2, Guava and BouncyCastle for seven endpoints |
| `jdbi3-spring`, `jdbi3-sqlobject` | Reject | One transaction model, and the SQL stays visible in text blocks |
| Picocli 4.7.7, Failsafe 3.3.2 | Reject | No arguments to parse, and both without a release in over a year |

Commits, in this order. The services move first while the Go checker still judges them, so the system and the oracle never change in the same step:

1. Ledger, relay and projector on Jdbi, behavior unchanged. Every test asserts what it asserted before, and the Go checker passes the correct mode on `all` and fails each naive mode on its set.
2. `checker` in Java. Each corruption test ported one to one, stats equal to the Go checker on the same histories.
3. `chaos` in Java, and the last Go out of the Makefile, CI, `.env` and Sonar, since deleting `chaos/` breaks every line that still names it.
4. A `jlink` runtime for the two tools, shared by both images.
5. A new experiment batch, `results/sample`, and the README and SDD tables from it.

Done when: no Go in the repo, `./mvnw verify` runs every test, and the new batch gives every mode its expected verdict.

### Phase 7: Write-up and launch

* README opens with the result table: correct mode with zero violations, naive modes with what they lost.
* `docs/INVARIANTS.md` explains each rule with the SQL that checks it.
* Posts on r/java, Hacker News and LinkedIn. Fintech recruiters read this kind of post.

Done when: README and posts reviewed by Jader.

---

## 5. Skills and their roles

| Skill | Role in this project | When |
|---|---|---|
| `java-engineer` | Domain model, outbox, idempotent consumer, locking, Spring Kafka config. Pattern table: Idempotency, Outbox, Dead Letter. | Before each Java part |
| `java-code-review` (`/code-review`) | Panel review per part, with the financial compliance lens. | Before each part merges |
| `engineering:system-design` | Check the event flow and the failure table before Phase 2. | Phase 1 end |
| `engineering:testing-strategy` | Split between property, Testcontainers and chaos tests. | Phase 1 |
| `security-review` | Input validation on the API, secrets, workflow permissions. | Phase 7 |
| `simplify` | Keep the naive modes small and obvious. | Phase 4 |
| `dataviz` | Report page charts and colors. | Phase 5 |
| `tech-design-writer` | `SDD.md`, `INVARIANTS.md`. | Phases 0 and 7 |
| `jader-voice` | README, commit messages, posts. | Every phase, final pass in Phase 7 |
| `run` | Start the stack and confirm a change works. | Any phase |

---

## 6. Best practices

### 6.1 Money and data

* Integer minor units and ISO 4217 currency. No `double`, no `float`. `Math.addExact` so an overflow throws instead of wrapping.
* One currency per transfer. No FX in this project.
* Postings are append only. A correction is a new transfer, never an update.
* Balance stored on `account` for speed and always checkable against the postings (I2).
* Database constraints as the last line: `check` on balance, deferred trigger on posting sums, trigger that blocks changes to postings, primary key on the idempotency key.
* Typed ids instead of raw `UUID` or `String`, so an account id and a transfer id can never swap places.

### 6.2 Idempotency

* Key from the client in the `Idempotency-Key` header, stored with a hash of the request body.
* Same key and same body: return the stored result. Same key and another body: `422`.
* Rejected transfers (insufficient funds) are stored too, so a retry gets the same rejection.
* Concurrent duplicates are serialized by the primary key of the `idempotency` table, not by application locks.
* The request hash covers the parsed fields in a fixed order, not the raw body.

### 6.3 Concurrency

* One transaction per transfer, `READ COMMITTED`, both accounts locked with `SELECT ... FOR UPDATE` in id order. No deadlocks by design.
* `SERIALIZABLE` with retry as the comparison in Phase 5. The retry wraps the whole transaction, never a catch inside it.
* Timeouts on every call: statement timeout in Postgres, socket timeout in the JDBC driver (the only one that works when Postgres is frozen), client timeout in the chaos runner, producer timeouts in the relay.

### 6.4 Messaging

* Outbox row in the same transaction as the postings. No dual write.
* At-least-once from the relay, dedup in the consumer. Exactly-once effect, not exactly-once delivery.
* Event payload carries the event id, transfer id and postings, so the projector never calls back the ledger.
* Postings commute, so the projector does not depend on order across partitions.
* The projector never skips a record on a database error: unlimited backoff, DLT only for records that cannot be parsed.
* The relay's producer timeouts stay below the database's idle-in-transaction timeout, so a Kafka outage never leaves rows locked.

### 6.5 Testing

* Property tests for the domain, Testcontainers with Toxiproxy for integration, chaos runs for the whole system.
* Every chaos fault has a test that shows the checker detects its naive counterpart.
* Fixed random seed per chaos run, printed in the report, so a failing run can be replayed.

### 6.6 Repo

* Commits straight to `main`, one short present-tense paragraph.
* Versions pinned in `.env` and the build files.
* CLAUDE.md stays local (global gitignore).

---

## 7. Tools

| Tool | Use |
|---|---|
| Docker Compose | Local runs and CI |
| Toxiproxy | Latency, resets and cuts between services and Postgres or Kafka |
| Docker Engine API | Kill, stop and pause containers from the chaos runner |
| Porcupine | Linearizability check of balance reads (stretch goal) |
| jqwik | Property tests on the domain |
| Testcontainers | Postgres, Kafka and Toxiproxy in JVM tests |
| Flyway | Ledger and projector schemas |
| Chaos Mesh | Optional Kubernetes chaos |
| SonarQube Cloud | Quality gate, JaCoCo coverage |
| GitHub Actions | Build, tests, nightly short chaos run per mode |

Versions are in [SDD §7](SDD.md#7-libraries-and-versions).

---

## 8. Open questions

| # | Question | Why it matters | Default assumed in this plan |
|---|---|---|---|
| Q1 | Debezium CDC instead of the polling relay? | Common in production, more moving parts. | Polling relay. Debezium as a later mode. |
| Q2 | Multi-currency transfers with FX? | More realistic, much more code. | No. |
| Q3 | Postgres failover with a replica (Patroni)? | Real failover tests lost commits after promotion. | No. Single node restart only. |
| Q4 | Porcupine linearizability check in v1? | Strong result, more work. | Stretch goal. |
| Q5 | Second ledger in Go for comparison? | Bigger audience, doubles the work. | No. |
