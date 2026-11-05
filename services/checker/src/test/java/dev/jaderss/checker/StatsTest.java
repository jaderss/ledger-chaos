package dev.jaderss.checker;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import dev.jaderss.checker.Stats.FaultWindow;
import dev.jaderss.checker.Stats.Second;
import java.util.stream.Stream;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import tools.jackson.core.JacksonException;

class StatsTest {

    private static final String HISTORY = """
            {"t":500,"worker":-1,"key":"fund","phase":"invoke"}
            {"t":600,"worker":-1,"key":"fund","phase":"ok"}
            {"t":1000,"worker":0,"key":"a","phase":"invoke"}
            {"t":1100,"worker":0,"key":"a","phase":"ok"}
            {"t":1200,"worker":1,"key":"b","phase":"invoke"}
            {"t":1500,"worker":2,"key":"a","phase":"invoke","mismatch":true}
            {"t":1510,"worker":2,"key":"a","phase":"fail","mismatch":true}
            {"t":2200,"worker":1,"key":"b","phase":"unknown"}
            {"t":2300,"worker":1,"key":"b","phase":"invoke"}
            {"t":3500,"worker":1,"key":"b","phase":"ok"}
            """;

    private static final String FAULTS = """
            {"t":1500,"fault":"pause-db","phase":"start"}
            {"t":2000,"fault":"pause-db","phase":"end"}
            {"t":3000,"fault":"db-cut","phase":"start"}
            """;

    @Test
    @DisplayName("counts, latencies, the timeline and fault windows come from the history")
    void readsStats() {
        var stats = Stats.read(HISTORY.lines(), FAULTS.lines(), Checker.json());

        assertThat(stats)
                .returns(2.5, Stats::seconds)
                .returns(2, Stats::keys)
                .returns(4, Stats::attempts)
                .returns(2, Stats::ok)
                .returns(1, Stats::fail)
                .returns(1, Stats::unknown)
                .returns(0.8, Stats::okPerSecond)
                .returns(100L, Stats::p50Ms)
                .returns(1200L, Stats::p99Ms)
                .returns(1200L, Stats::maxMs);
        assertThat(stats.timeline())
                .containsExactly(new Second(0, 1, 1, 0), new Second(1, 0, 0, 1), new Second(2, 1, 0, 0));
        assertThat(stats.faults())
                .containsExactly(new FaultWindow("pause-db", 0.5, 1), new FaultWindow("db-cut", 2, 2.5));
    }

    @Test
    @DisplayName("an empty history gives one empty second, a broken line is an error")
    void emptyOrBroken() {
        var stats = Stats.read(Stream.empty(), Stream.empty(), Checker.json());

        assertThat(stats.attempts()).isZero();
        assertThat(stats.p99Ms()).isZero();
        assertThat(stats.faults()).isEmpty();
        assertThat(stats.timeline()).hasSize(1);
        assertThatThrownBy(() -> Stats.read(Stream.of("not json"), Stream.empty(), Checker.json()))
                .isInstanceOf(JacksonException.class);
    }
}
