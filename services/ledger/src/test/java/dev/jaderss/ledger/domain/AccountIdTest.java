package dev.jaderss.ledger.domain;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.UUID;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class AccountIdTest {

    private static AccountId id(String value) {
        return new AccountId(UUID.fromString(value));
    }

    @Test
    @DisplayName("account ids sort byte by byte, like Postgres sorts uuid, not like UUID.compareTo")
    void unsignedOrder() {
        var low = id("00000000-0000-0000-0000-000000000001");
        var high = id("80000000-0000-0000-0000-000000000000");
        var lowTail = id("00000000-0000-0000-0000-000000000000");
        var highTail = id("00000000-0000-0000-8000-000000000000");

        assertThat(low.value()).isGreaterThan(high.value());
        assertThat(low).isLessThan(high);
        assertThat(lowTail).isLessThan(low).isLessThan(highTail);
        assertThat(low).isLessThan(highTail);
        assertThat(low).isEqualByComparingTo(id(low.toString()));
    }
}
