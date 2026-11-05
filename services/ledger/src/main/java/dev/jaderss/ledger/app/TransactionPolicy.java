package dev.jaderss.ledger.app;

import org.springframework.transaction.TransactionDefinition;

record TransactionPolicy(int isolationLevel, int retries) {

    static TransactionPolicy readCommitted() {
        return new TransactionPolicy(TransactionDefinition.ISOLATION_READ_COMMITTED, 0);
    }

    static TransactionPolicy serializable(int retries) {
        return new TransactionPolicy(TransactionDefinition.ISOLATION_SERIALIZABLE, retries);
    }
}
