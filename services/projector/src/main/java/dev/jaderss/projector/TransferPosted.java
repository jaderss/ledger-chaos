package dev.jaderss.projector;

import java.time.Instant;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.UUID;

record TransferPosted(
        UUID eventId,
        String type,
        UUID transferId,
        String correlationId,
        Instant occurredAt,
        String currency,
        List<Line> postings) {

    record Line(UUID accountId, long amountMinor) {
        Line {
            if (accountId == null) {
                throw new IllegalArgumentException("posting without an account id");
            }
        }
    }

    TransferPosted {
        Objects.requireNonNull(eventId, "eventId");
        Objects.requireNonNull(currency, "currency");
        if (!currency.matches("[A-Z]{3}")) {
            throw new IllegalArgumentException("event " + eventId + " has currency " + currency);
        }
        if (!"TransferPosted".equals(type)) {
            throw new IllegalArgumentException("unknown event type " + type);
        }
        if (postings == null || postings.isEmpty()) {
            throw new IllegalArgumentException("event " + eventId + " has no postings");
        }
        if (postings.stream().mapToLong(Line::amountMinor).reduce(0, Math::addExact) != 0) {
            throw new IllegalArgumentException("postings of event " + eventId + " do not sum to zero");
        }
        postings = List.copyOf(postings);
    }

    List<Line> postingsInAccountOrder() {
        return postings.stream()
                .sorted(Comparator.comparing(line -> line.accountId().toString()))
                .toList();
    }
}
