package dev.jaderss.checker;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.util.Map;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import tools.jackson.databind.JsonNode;

class CheckerTest {

    private static final String HISTORY = """
            {"op":"transfer","key":"k1","phase":"invoke","body":{"from":"%1$s","to":"%2$s","amount":40,"currency":"EUR"}}
            {"op":"transfer","key":"k1","phase":"ok","status":201,"transferId":"%3$s","transferStatus":"POSTED"}
            """.formatted(StoresTest.A, StoresTest.B, StoresTest.T1);

    @TempDir
    Path runDir;

    private final ByteArrayOutputStream out = new ByteArrayOutputStream();

    private boolean runFixture(boolean projectorCaughtUp) throws Exception {
        var ledger = Containers.database(StoresTest.ledgerSchema(true));
        var projector = Containers.database(StoresTest.projectorSchema(-40, 40));
        try (var admin = Containers.admin()) {
            var topic = Containers.topic(admin, 1);
            var deadLetters = Containers.topic(admin, 1);
            Containers.produce(topic, StoresTest.event("e1"));
            var group = topic + "-group";
            if (projectorCaughtUp) {
                Containers.commitToEnd(admin, group, topic);
            }
            Files.writeString(runDir.resolve("history.jsonl"), HISTORY);
            Files.writeString(runDir.resolve("run.json"), "{\"seed\":7}");
            var config = Config.from(Map.of(
                    "RUN_DIR",
                    runDir.toString(),
                    "LEDGER_DSN",
                    Containers.dsn(ledger),
                    "PROJECTOR_DSN",
                    Containers.dsn(projector),
                    "KAFKA_BROKERS",
                    Containers.brokers(),
                    "TOPIC",
                    topic,
                    "DLT_TOPIC",
                    deadLetters,
                    "GROUP",
                    group,
                    "QUIET_TIMEOUT",
                    "1s"));
            return Checker.run(config, new PrintStream(out, true, StandardCharsets.UTF_8));
        }
    }

    private String output() {
        return out.toString(StandardCharsets.UTF_8);
    }

    @Test
    @DisplayName("a clean system passes and the report carries the run, the stats and every result")
    void cleanSystemPasses() throws Exception {
        assertThat(runFixture(true)).as(this::output).isTrue();
        assertThat(output()).contains("verdict: correct passes every invariant");

        JsonNode report = Checker.json().readTree(runDir.resolve("report.json").toFile());
        assertThat(report.get("ok").asBoolean()).isTrue();
        assertThat(report.get("quiet").asBoolean()).isTrue();
        assertThat(report.get("locking").asString()).isEqualTo("ordered");
        assertThat(report.get("stats").get("ok").asInt()).isOne();
        assertThat(report.get("run").get("seed").asInt()).isEqualTo(7);
        assertThat(report.get("results")).hasSize(10);
    }

    @Test
    @DisplayName("a projector that never catches up fails I8 and I9")
    void projectorNeverCatchesUp() throws Exception {
        assertThat(runFixture(false)).as(this::output).isFalse();
        assertThat(output()).contains("I8   FAIL", "projector lag 1 after 1s");
    }

    @Test
    @DisplayName("an unknown mode is refused before anything is read")
    void unknownMode() {
        assertThatThrownBy(() -> Config.from(Map.of("LEDGER_MODE", "corect"))).hasMessageContaining("corect");
    }

    @Test
    @DisplayName("a run needs its history")
    void needsHistory() {
        var config = Config.from(Map.of("RUN_DIR", runDir.toString()));

        assertThatThrownBy(() -> Checker.run(config, new PrintStream(out, true, StandardCharsets.UTF_8)))
                .isInstanceOf(NoSuchFileException.class);
    }
}
