# ledger-chaos

A double-entry ledger in Spring Boot, plus a Go chaos runner and checker that try to make it lose money.

The runner sends thousands of transfers from 64 workers. It also kills the services, pauses Postgres, cuts the network and stops Kafka. After the run, the checker reads both databases, the Kafka topic and the client history, and checks 10 invariants.

The same runs go against four naive versions of the ledger. Each one is the correct code with one step changed. The checker must catch every one of them.

## Results

60 second runs on a laptop, 64 workers, 20 accounts.

| Mode | Faults | Transfers sent | Result |
|---|---|---|---|
| `correct` | every fault in the catalog, one at a time | 96,180 | all invariants pass |
| `correct`, `SERIALIZABLE` | every fault in the catalog, one at a time | 24,560 | all invariants pass |
| `correct` | 1.5 s database latency, 8 s at a time | 47,797 | all invariants pass |
| `no-idempotency` | 1.5 s database latency, 8 s at a time | 46,267 | 2,911 acknowledged transfers posted more than once |
| `dual-write` | Kafka stopped and started | 9,790 | 1,015 posted transfers never reached the topic |
| `unordered-locks` | none, lock contention only | 2,030 | 101 deadlocks |
| `read-modify-write` | none, lock contention only | 133,417 | 20 stored balances differ from their postings, 5 accounts overspent |

What a naive run looks like, from the checker:

```
mode dual-write: 9810 keys, 9623 ok, 90 fail, 3618 unknown, 574 keys never answered, 77 mismatch attempts, 9636 transfers, 8532 events
I8   FAIL   1015  every posted transfer reaches the topic, and only those
            posted transfer aa87a822-70e0-4a17-a54a-fee2b984fcf8 is not on the topic
I9   FAIL     20  projector balances equal the ledger's sums of postings
            account 08bbd43d-9dbe-4d6f-b62e-1a6ceb4a2776 projected 212909, ledger postings sum to 211792
145 ok/s over 66 s, p50 47 ms, p99 201 ms, 22350 commits, 0 rollbacks, 7 faults
verdict: dual-write fails I8, I9, and must fail I8, I9
```

## Ordered locks against SERIALIZABLE

The same correct ledger, two ways to stop concurrent transfers from losing money, no faults:

| Accounts | Locking | ok/s | p99 ms | Rollbacks |
|---|---|---|---|---|
| 20 | `FOR UPDATE` in id order | 1,914 | 104 | 0 |
| 20 | `SERIALIZABLE`, retry up to 5 times | 956 | 195 | 121,093 |
| 200 | `FOR UPDATE` in id order | 2,797 | 59 | 0 |
| 200 | `SERIALIZABLE`, retry up to 5 times | 2,089 | 91 | 33,314 |

Ordered locks win at both levels. A transfer waits for the row, while a serializable transaction does the work, then throws it away when it loses. With 20 accounts that happens about twice per ok answer. Both pass every invariant.

## How the correct mode works

One `POST /transfers` is one Postgres transaction, `READ COMMITTED`:

1. Claim the `Idempotency-Key` with an insert on the primary key. If the key exists, return the stored answer, or `422` if the body is different.
2. Lock both accounts with `select ... for update`, always in account id order.
3. Decide: post, or reject for insufficient funds.
4. Write the transfer, two postings, the new balances, and one outbox row.

A relay publishes the outbox to Kafka at least once. A projector applies each event once, deduplicated by event id, into its own balance table.

```
client ──► ledger ──► Postgres (transfer, postings, outbox)
                           │
                         relay ──► Kafka ──► projector ──► Postgres (balances)
```

## The naive modes

| Mode | What changes | Invariants it must fail |
|---|---|---|
| `no-idempotency` | Skips the key claim, so a retry posts again | I4, I6, I7 |
| `dual-write` | No outbox. Sends to Kafka after the commit | I8, I9 |
| `unordered-locks` | Locks `from` then `to`, not by id | I11 |
| `read-modify-write` | Reads balances without a lock and writes them back | I2 (also breaks I3 when an account overspends) |

Each mode is one line in `TransferServiceConfig`, so the diff from the correct mode is the bug.

## Invariants

| | Rule |
|---|---|
| I1 | The sum of all postings per currency is zero |
| I2 | Each account balance equals the sum of its postings |
| I3 | No account without overdraft goes below zero |
| I4 | A transfer acknowledged to the client exists exactly once |
| I5 | A transfer refused with a definite error does not exist |
| I6 | A transfer with unknown outcome (timeout, reset) exists at most once |
| I7 | Same key and body give the same answer. Same key and another body give `422` and change nothing |
| I8 | Every posted transfer reaches the topic, and every event matches a posted transfer |
| I9 | Projector balances equal the ledger's sums of postings |
| I11 | No transfer transaction ends in a deadlock |

I10 (linearizable balance reads, with Porcupine) is not built yet.

## Run it

Needs Docker and a JDK 25. Go runs inside containers.

```bash
make up                                          # Postgres, Kafka, Toxiproxy, roles, topics
make test                                        # Java and Go tests
make chaos MODE=correct SCENARIO=all             # one run, results in results/<run id>
make chaos MODE=dual-write SCENARIO=kafka-down
make chaos-all                                   # every mode and the locking comparison
make report                                      # results page on http://localhost:8090
make down
```

Scenarios: `contention`, `db-latency`, `kafka-down`, `kills`, `db-faults`, `all`. The checker exits `0` when the mode fails exactly as it should: no failures for `correct`, at least its set for a naive mode.

## Layout

| Path | What |
|---|---|
| `services/ledger` | Transfer API, idempotency, locking, postings, outbox |
| `services/relay` | Outbox to Kafka |
| `services/projector` | Idempotent consumer and read model |
| `chaos` | Go workload and fault injector, standard library only |
| `checker` | Go invariant checks over both databases, the topic and the history |
| `report` | Results page: every run, invariants per run, the locking comparison, faults over time |
| `results/sample` | The `report.json` of the runs above |
| `docs/SDD.md` | Design, algorithms, fault catalog, versions |

## What it skips

One currency per transfer, no FX, fees or users. Single Postgres node, no failover. Not a production ledger: it exists to show which of these patterns hold under faults and which do not.
