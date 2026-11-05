package dev.jaderss.ledger.domain;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class MoneyTest {

    @Test
    @DisplayName("adds amounts of the same currency")
    void adds() {
        assertThat(Money.of(250, "EUR").plus(Money.of(50, "EUR"))).isEqualTo(Money.of(300, "EUR"));
    }

    @Test
    @DisplayName("an overflowing sum throws instead of wrapping")
    void overflowThrows() {
        var max = Money.of(Long.MAX_VALUE, "EUR");
        assertThatThrownBy(() -> max.plus(Money.of(1, "EUR"))).isInstanceOf(ArithmeticException.class);
    }

    @Test
    @DisplayName("an overflowing difference throws instead of wrapping")
    void underflowThrows() {
        var min = Money.of(Long.MIN_VALUE, "EUR");
        assertThatThrownBy(() -> min.minus(Money.of(1, "EUR"))).isInstanceOf(ArithmeticException.class);
    }

    @Test
    @DisplayName("refuses to add two currencies")
    void currencyMismatch() {
        assertThatThrownBy(() -> Money.of(1, "EUR").plus(Money.of(1, "USD")))
                .isInstanceOf(CurrencyMismatchException.class);
    }

    @Test
    @DisplayName("rejects an unknown currency code")
    void unknownCurrency() {
        assertThatThrownBy(() -> Money.of(1, "XXQ")).isInstanceOf(IllegalArgumentException.class);
    }
}
