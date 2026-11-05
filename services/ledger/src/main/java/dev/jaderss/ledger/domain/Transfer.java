package dev.jaderss.ledger.domain;

import java.time.Instant;
import java.util.Objects;
import java.util.Optional;

public record Transfer(
        TransferId id,
        IdempotencyKey key,
        String correlationId,
        AccountId from,
        AccountId to,
        Money amount,
        TransferStatus status,
        RejectReason rejectReason,
        Instant createdAt) {

    public Transfer {
        Objects.requireNonNull(id, "id");
        Objects.requireNonNull(key, "key");
        Objects.requireNonNull(status, "status");
        if ((status == TransferStatus.REJECTED) != (rejectReason != null)) {
            throw new IllegalArgumentException("a reject reason is required for, and only for, rejected transfers");
        }
    }

    public Optional<RejectReason> reason() {
        return Optional.ofNullable(rejectReason);
    }
}
