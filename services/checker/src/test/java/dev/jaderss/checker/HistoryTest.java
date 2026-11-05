package dev.jaderss.checker;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import dev.jaderss.checker.History.Body;
import dev.jaderss.checker.History.KeyHistory;
import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.stream.Stream;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class HistoryTest {

    private static Map<String, KeyHistory> read(String... lines) {
        return History.read(Stream.of(lines), Checker.json());
    }

    @Test
    @DisplayName("answers and mismatches are grouped by key, other operations are ignored")
    void groupsByKey() {
        var keys = read("""
                {"op":"transfer","key":"k","phase":"invoke","body":{"from":"a","to":"b","amount":5,"currency":"EUR"}}""", """
                {"op":"transfer","key":"k","phase":"unknown","error":"timeout"}""", """
                {"op":"transfer","key":"k","phase":"invoke","body":{"from":"a","to":"b","amount":5,"currency":"EUR"}}""", """
                {"op":"transfer","key":"k","phase":"ok","status":200,"transferId":"t","transferStatus":"POSTED","replayed":true}""", """
                {"op":"transfer","key":"k","phase":"invoke","mismatch":true,"body":{"from":"a","to":"b","amount":6,"currency":"EUR"}}""", """
                {"op":"transfer","key":"k","phase":"fail","mismatch":true,"status":422,"problem":"idempotency-key-mismatch"}""", """
                {"op":"read","key":"ignored","phase":"ok"}""");

        assertThat(keys).containsOnlyKeys("k");
        var k = keys.get("k");
        assertThat(k.body()).isEqualTo(new Body("a", "b", 5, "EUR"));
        assertThat(k.answers()).hasSize(2);
        assertThat(k.posted()).isTrue();
        assertThat(k.definite()).hasSize(1);
        assertThat(k.mismatches()).singleElement().satisfies(m -> {
            assertThat(m.body().amount()).isEqualTo(6);
            assertThat(m.answer().problem()).isEqualTo("idempotency-key-mismatch");
        });
    }

    @Test
    @DisplayName("a broken line or a mismatch result without its invoke is an error")
    void rejectsBrokenLines() {
        assertThatThrownBy(() -> read("{broken")).hasMessageStartingWith("history line 1");
        assertThatThrownBy(() -> read("""
                {"op":"transfer","key":"k","phase":"fail","mismatch":true}""")).hasMessage("history line 1: mismatch result without invoke");
    }

    @Test
    @DisplayName("an event sums its postings per account, an unparseable one keeps its position")
    void parsesEvents() {
        var event = TopicReader.parse("""
                {"eventId":"e","transferId":"t","postings":[{"accountId":"a","amountMinor":-5},{"accountId":"b","amountMinor":5}]}""".getBytes(StandardCharsets.UTF_8), "0/1", Checker.json());
        var bad = TopicReader.parse("nope".getBytes(StandardCharsets.UTF_8), "2/7", Checker.json());

        assertThat(event).isEqualTo(new Snapshot.Event("e", "t", Map.of("a", -5L, "b", 5L)));
        assertThat(bad).isEqualTo(new Snapshot.Event("unparseable at 2/7", "", Map.of()));
    }

    @Test
    @DisplayName("the summary marks failures, unanswered keys and examples")
    void printMarksFailures() {
        var fixture = SnapshotFixture.clean();
        fixture.deadlocks = 1;
        var snapshot = fixture.build();
        var out = new ByteArrayOutputStream();

        Checker.print(new PrintStream(out, true, StandardCharsets.UTF_8), snapshot, Invariants.check(snapshot));

        assertThat(out.toString(StandardCharsets.UTF_8)).contains("I11  FAIL", "1 keys never answered", "1 deadlocks");
    }
}
