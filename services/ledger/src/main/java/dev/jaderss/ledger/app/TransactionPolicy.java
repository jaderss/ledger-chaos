package dev.jaderss.ledger.app;

import dev.jaderss.ledger.persistence.Isolation;

record TransactionPolicy(Isolation isolation, int retries) {

    static TransactionPolicy readCommitted() {
        return new TransactionPolicy(Isolation.READ_COMMITTED, 0);
    }

    static TransactionPolicy serializable(int retries) {
        return new TransactionPolicy(Isolation.SERIALIZABLE, retries);
    }
}
