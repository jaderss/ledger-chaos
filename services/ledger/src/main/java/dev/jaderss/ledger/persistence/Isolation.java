package dev.jaderss.ledger.persistence;

import java.util.Optional;

public enum Isolation {
    READ_COMMITTED,
    SERIALIZABLE;

    Optional<String> firstStatement() {
        return switch (this) {
            case READ_COMMITTED -> Optional.empty();
            case SERIALIZABLE -> Optional.of("set transaction isolation level serializable");
        };
    }
}
