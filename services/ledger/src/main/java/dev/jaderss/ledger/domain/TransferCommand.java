package dev.jaderss.ledger.domain;

import java.util.Objects;

public record TransferCommand(IdempotencyKey key, String correlationId, AccountId from, AccountId to, Money amount) {

    public TransferCommand {
        Objects.requireNonNull(key, "key");
        Objects.requireNonNull(correlationId, "correlationId");
        Objects.requireNonNull(from, "from");
        Objects.requireNonNull(to, "to");
        Objects.requireNonNull(amount, "amount");
        if (from.equals(to)) {
            throw new IllegalArgumentException("from and to must be different accounts");
        }
        if (!amount.isPositive()) {
            throw new IllegalArgumentException("amount must be positive");
        }
    }

    public RequestHash hash() {
        return RequestHash.of(from, to, amount);
    }
}
