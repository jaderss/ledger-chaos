package dev.jaderss.ledger.domain;

import java.util.Objects;
import java.util.UUID;

public record AccountId(UUID value) implements Comparable<AccountId> {

    public AccountId {
        Objects.requireNonNull(value, "value");
    }

    @Override
    public int compareTo(AccountId other) {
        var high = Long.compareUnsigned(value.getMostSignificantBits(), other.value.getMostSignificantBits());
        return high != 0
                ? high
                : Long.compareUnsigned(value.getLeastSignificantBits(), other.value.getLeastSignificantBits());
    }

    @Override
    public String toString() {
        return value.toString();
    }
}
