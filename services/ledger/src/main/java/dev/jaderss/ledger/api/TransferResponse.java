package dev.jaderss.ledger.api;

import dev.jaderss.ledger.domain.Transfer;
import java.time.Instant;
import java.util.UUID;

record TransferResponse(
        UUID id,
        String idempotencyKey,
        String correlationId,
        UUID fromAccount,
        UUID toAccount,
        long amountMinor,
        String currency,
        String status,
        String rejectReason,
        Instant createdAt) {

    static TransferResponse from(Transfer transfer) {
        return new TransferResponse(
                transfer.id().value(),
                transfer.key().value(),
                transfer.correlationId(),
                transfer.from().value(),
                transfer.to().value(),
                transfer.amount().amountMinor(),
                transfer.amount().currency().getCurrencyCode(),
                transfer.status().name(),
                transfer.reason().map(Enum::name).orElse(null),
                transfer.createdAt());
    }
}
