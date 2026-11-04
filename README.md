# ledger-chaos

A double-entry ledger with idempotency keys, a transactional outbox and an idempotent consumer. A chaos runner sends transfers while it kills processes, cuts the network and pauses the database. A checker then proves that no cent was lost, created or applied twice.

The same chaos runs against naive versions of the ledger (no idempotency, dual write, unordered locks, read-modify-write). The checker must catch each one.

Status: design. No results yet.

* [Plan](docs/PLAN.md): invariants, phases, tools.
* [SDD](docs/SDD.md): design, API, algorithms, scaffolding, versions.
