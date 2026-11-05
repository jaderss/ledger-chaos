package dev.jaderss.chaos;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;
import java.util.Map;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

class ConfigTest {

    @Test
    @DisplayName("settings come from the environment")
    void readsTheEnvironment() {
        var config = Config.from(
                Map.of("SCENARIO", "kills", "WORKERS", "3", "DURATION", "5s", "DUPLICATE_RATE", "0.5", "SEED", "42"));

        assertThat(config.faults().scenario()).isEqualTo(Scenario.KILLS);
        assertThat(config.load().workers()).isEqualTo(3);
        assertThat(config.duration()).isEqualTo(Duration.ofSeconds(5));
        assertThat(config.load().duplicateRate()).isEqualTo(0.5);
        assertThat(config.seed()).isEqualTo(42);
    }

    @Test
    @DisplayName("an unknown scenario, a bad number and too few accounts are all reported")
    void rejectsBadSettings() {
        assertThatThrownBy(() -> Config.from(Map.of("SCENARIO", "nope", "WORKERS", "many", "ACCOUNTS", "1")))
                .hasMessageContaining("SCENARIO")
                .hasMessageContaining("WORKERS")
                .hasMessageContaining("ACCOUNTS needs at least 2");
    }

    @ParameterizedTest
    @CsvSource({
        "60s, PT1M, 1m0s",
        "1500ms, PT1.5S, 1.5s",
        "1m30s, PT1M30S, 1m30s",
        "300ms, PT0.3S, 300ms",
        "2h, PT2H, 2h0m0s"
    })
    @DisplayName("durations parse and print like Go")
    void goDurations(String text, Duration expected, String printed) {
        assertThat(GoDuration.parse(text)).isEqualTo(expected);
        assertThat(GoDuration.format(expected)).isEqualTo(printed);
    }

    @Test
    @DisplayName("a duration without a unit is refused")
    void durationNeedsAUnit() {
        assertThatThrownBy(() -> GoDuration.parse("60")).hasMessageContaining("not a duration");
        assertThatThrownBy(() -> GoDuration.parse("5sx")).hasMessageContaining("not a duration");
    }
}
