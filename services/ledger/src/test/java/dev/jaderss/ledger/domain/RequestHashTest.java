package dev.jaderss.ledger.domain;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.UUID;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class RequestHashTest {

    private final AccountId from = new AccountId(UUID.randomUUID());
    private final AccountId to = new AccountId(UUID.randomUUID());

    @Test
    @DisplayName("the same fields give the same hash")
    void stable() {
        assertThat(RequestHash.of(from, to, Money.of(100, "EUR")))
                .isEqualTo(RequestHash.of(from, to, Money.of(100, "EUR")));
    }

    @Test
    @DisplayName("a round trip through bytes keeps the hash equal")
    void roundTrip() {
        var hash = RequestHash.of(from, to, Money.of(100, "EUR"));
        assertThat(RequestHash.fromBytes(hash.bytes())).isEqualTo(hash).hasSameHashCodeAs(hash);
    }

    @Test
    @DisplayName("the amount is part of the hash")
    void amount() {
        assertThat(RequestHash.of(from, to, Money.of(100, "EUR")))
                .isNotEqualTo(RequestHash.of(from, to, Money.of(101, "EUR")));
    }

    @Test
    @DisplayName("the currency is part of the hash")
    void currency() {
        assertThat(RequestHash.of(from, to, Money.of(100, "EUR")))
                .isNotEqualTo(RequestHash.of(from, to, Money.of(100, "USD")));
    }

    @Test
    @DisplayName("the direction is part of the hash")
    void direction() {
        assertThat(RequestHash.of(from, to, Money.of(100, "EUR")))
                .isNotEqualTo(RequestHash.of(to, from, Money.of(100, "EUR")));
    }
}
