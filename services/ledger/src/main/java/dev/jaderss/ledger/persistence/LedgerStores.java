package dev.jaderss.ledger.persistence;

import org.jdbi.v3.core.Handle;
import tools.jackson.databind.json.JsonMapper;

public record LedgerStores(
        Handle handle,
        IdempotencyStore idempotency,
        AccountStore accounts,
        TransferStore transfers,
        PostingStore postings,
        OutboxStore outbox) {

    static LedgerStores on(Handle handle, JsonMapper json) {
        return new LedgerStores(
                handle,
                new IdempotencyStore(handle),
                new AccountStore(handle),
                new TransferStore(handle),
                new PostingStore(handle),
                new OutboxStore(handle, json));
    }

    public void afterCommit(Runnable action) {
        handle.afterCommit(action);
    }
}
