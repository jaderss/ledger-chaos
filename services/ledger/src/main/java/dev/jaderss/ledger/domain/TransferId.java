package dev.jaderss.ledger.domain;

import java.util.Objects;
import java.util.UUID;

public record TransferId(UUID value) {

    public TransferId {
        Objects.requireNonNull(value, "value");
    }

    public static TransferId random() {
        return new TransferId(UUID.randomUUID());
    }

    @Override
    public String toString() {
        return value.toString();
    }
}
