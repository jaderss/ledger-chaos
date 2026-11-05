package dev.jaderss.ledger.persistence;

import org.springframework.stereotype.Component;

@Component
public record LedgerStores(
        IdempotencyStore idempotency,
        AccountStore accounts,
        TransferStore transfers,
        PostingStore postings,
        OutboxStore outbox) {}
