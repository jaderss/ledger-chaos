package dev.jaderss.checker;

import com.fasterxml.jackson.annotation.JsonInclude;
import dev.jaderss.checker.History.KeyHistory;
import dev.jaderss.checker.Snapshot.Quiet;
import dev.jaderss.checker.Snapshot.Topic;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import tools.jackson.databind.DeserializationFeature;
import tools.jackson.databind.SerializationFeature;
import tools.jackson.databind.json.JsonMapper;

public final class Checker {

    private static final Duration POLL = Duration.ofSeconds(1);
    private static final JsonMapper JSON = JsonMapper.builder()
            .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
            .disable(DeserializationFeature.FAIL_ON_NULL_FOR_PRIMITIVES)
            .enable(SerializationFeature.INDENT_OUTPUT)
            .build();

    private Checker() {}

    @JsonInclude(JsonInclude.Include.NON_NULL)
    record Report(
            String mode,
            String locking,
            List<String> mustFail,
            boolean ok,
            String verdict,
            boolean quiet,
            String quietDetail,
            List<Result> results,
            Stats stats,
            Map<String, Object> run) {}

    public static void main(String[] args) {
        try {
            if (!run(Config.from(System.getenv()), System.out)) {
                System.exit(1);
            }
        } catch (IOException | RuntimeException e) {
            System.err.println(e.getMessage());
            System.exit(1);
        }
    }

    static JsonMapper json() {
        return JSON;
    }

    static boolean run(Config config, PrintStream out) throws IOException {
        var keys = readHistory(config.runDir());
        var databases = Databases.connect(config.ledger(), config.projector());
        try (var kafka = new TopicReader(config.kafka().brokers(), JSON)) {
            var quiet = waitQuiet(databases, kafka, config);
            var ledger = databases.loadLedger();
            var projector = databases.projectorBalances();
            var topic = new Topic(
                    kafka.events(config.kafka().topic()),
                    kafka.count(config.kafka().deadLetterTopic()));
            var snapshot = new Snapshot(config.mode(), keys, ledger, topic, projector, quiet);
            var stats = readStats(config.runDir()).withCounters(ledger.counters());

            var results = Invariants.check(snapshot);
            print(out, snapshot, results);
            out.printf(
                    "%.0f ok/s over %.0f s, p50 %d ms, p99 %d ms, %d commits, %d rollbacks, %d faults%n",
                    stats.okPerSecond(),
                    stats.seconds(),
                    stats.p50Ms(),
                    stats.p99Ms(),
                    stats.commits(),
                    stats.rollbacks(),
                    stats.faults().size());
            var verdict = config.mode().verdict(results);
            out.println("verdict: " + verdict.text());
            writeReport(
                    config.runDir(),
                    new Report(
                            config.mode().label(),
                            config.locking(),
                            config.mode().mustFail(),
                            verdict.ok(),
                            verdict.text(),
                            quiet.reached(),
                            quiet.detail(),
                            results,
                            stats,
                            readRun(config.runDir())));
            return verdict.ok();
        }
    }

    private static Map<String, KeyHistory> readHistory(Path runDir) throws IOException {
        try (var lines = Files.lines(runDir.resolve("history.jsonl"))) {
            return History.read(lines, JSON);
        }
    }

    private static Stats readStats(Path runDir) throws IOException {
        var faults = runDir.resolve("faults.jsonl");
        try (var history = Files.lines(runDir.resolve("history.jsonl"));
                var faultLines = Files.exists(faults) ? Files.lines(faults) : Stream.<String>empty()) {
            return Stats.read(history, faultLines, JSON);
        }
    }

    private static Map<String, Object> readRun(Path runDir) {
        try {
            return JSON.readValue(runDir.resolve("run.json").toFile(), Map.class);
        } catch (RuntimeException _) {
            return null;
        }
    }

    private static void writeReport(Path runDir, Report report) {
        JSON.writeValue(runDir.resolve("report.json").toFile(), report);
    }

    static Quiet waitQuiet(Databases databases, TopicReader kafka, Config config) {
        var deadline = System.nanoTime() + config.quietTimeout().toNanos();
        while (true) {
            var errors = new StringBuilder();
            var rows = attempt(databases::unpublished, errors);
            var lag = attempt(
                    () -> kafka.lag(config.kafka().group(), config.kafka().topic()), errors);
            var detail = "%d unpublished outbox rows, projector lag %d".formatted(rows, lag);
            if (errors.isEmpty() && rows == 0 && lag == 0) {
                return new Quiet(true, detail);
            }
            if (!errors.isEmpty()) {
                detail += " (errors:" + errors + ")";
            }
            if (System.nanoTime() > deadline) {
                return new Quiet(false, detail + " after " + format(config.quietTimeout()));
            }
            sleep();
        }
    }

    private interface Probe {
        long read();
    }

    private static long attempt(Probe probe, StringBuilder errors) {
        try {
            return probe.read();
        } catch (RuntimeException e) {
            errors.append(' ').append(e.getMessage());
            return 0;
        }
    }

    private static String format(Duration duration) {
        return duration.toString().substring(2).toLowerCase();
    }

    private static void sleep() {
        try {
            Thread.sleep(POLL);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        }
    }

    static void print(PrintStream out, Snapshot s, List<Result> results) {
        var counts = new HashMap<String, Integer>();
        for (var k : s.keys().values()) {
            k.answers().forEach(a -> counts.merge(a.phase(), 1, Integer::sum));
            counts.merge("mismatch", k.mismatches().size(), Integer::sum);
            if (!k.answers().isEmpty() && k.definite().isEmpty()) {
                counts.merge("unanswered", 1, Integer::sum);
            }
        }
        out.printf(
                "mode %s: %d keys, %d ok, %d fail, %d unknown, %d keys never answered, %d mismatch attempts, %d transfers, %d events%n",
                s.mode().label(),
                s.keys().size(),
                counts.getOrDefault(History.OK, 0),
                counts.getOrDefault(History.FAIL, 0),
                counts.getOrDefault(History.UNKNOWN, 0),
                counts.getOrDefault("unanswered", 0),
                counts.getOrDefault("mismatch", 0),
                s.ledger().transfers().size(),
                s.topic().events().size());
        for (var r : results) {
            out.printf("%-4s %s %6d  %s%n", r.id(), r.pass() ? "PASS" : "FAIL", r.count(), r.rule());
            r.examples().forEach(e -> out.println("            " + e));
        }
    }
}
