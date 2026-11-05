package dev.jaderss.relay;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class RelayPropertiesTest {

    private static RelayProperties withIdleTimeout(Duration idle) {
        return new RelayProperties(
                "t",
                100,
                Duration.ofMillis(50),
                new RelayProperties.Kafka(Duration.ofSeconds(3), Duration.ofSeconds(3), Duration.ofSeconds(5)),
                idle);
    }

    @Test
    @DisplayName("refuses producer timeouts that can outlast the idle in transaction timeout")
    void refusesUnsafeTimeouts() {
        assertThatThrownBy(() -> withIdleTimeout(Duration.ofSeconds(8)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("must stay below");
    }

    @Test
    @DisplayName("the batch deadline is max block plus delivery timeout")
    void batchDeadline() {
        assertThat(withIdleTimeout(Duration.ofSeconds(10)).batchDeadline()).isEqualTo(Duration.ofSeconds(8));
    }
}
