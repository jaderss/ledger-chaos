package dev.jaderss.ledger.domain;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

public record TransferPosted(
        UUID eventId,
        String type,
        UUID transferId,
        String correlationId,
        Instant occurredAt,
        String currency,
        List<Line> postings) {

    public record Line(UUID accountId, long amountMinor) {}

    public TransferPosted {
        postings = List.copyOf(postings);
    }

    public static TransferPosted of(UUID eventId, Transfer transfer, List<Posting> postings) {
        return new TransferPosted(
                eventId,
                "TransferPosted",
                transfer.id().value(),
                transfer.correlationId(),
                transfer.createdAt(),
                transfer.amount().currency().getCurrencyCode(),
                postings.stream()
                        .map(p -> new Line(p.accountId().value(), p.amount().amountMinor()))
                        .toList());
    }
}
