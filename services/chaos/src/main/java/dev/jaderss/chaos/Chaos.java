package dev.jaderss.chaos;

import java.io.IOException;
import java.io.PrintStream;
import java.net.UnixDomainSocketAddress;
import java.nio.file.Files;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import tools.jackson.databind.DeserializationFeature;
import tools.jackson.databind.json.JsonMapper;

public final class Chaos {

    private static final JsonMapper JSON = JsonMapper.builder()
            .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
            .disable(DeserializationFeature.FAIL_ON_NULL_FOR_PRIMITIVES)
            .build();

    private Chaos() {}

    public static void main(String[] args) {
        try {
            run(Config.from(System.getenv()), System.err);
        } catch (IOException | RuntimeException e) {
            System.err.println(e.getMessage());
            System.exit(1);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            System.exit(1);
        }
    }

    static JsonMapper json() {
        return JSON;
    }

    static void run(Config config, PrintStream out) throws IOException, InterruptedException {
        Files.createDirectories(config.runDir());
        var targets = config.targets();
        var docker = new Docker(UnixDomainSocketAddress.of(targets.dockerSocket()), targets.project(), JSON);
        var catalog = Faults.catalog(
                docker,
                new Toxiproxy(targets.toxiproxy(), JSON),
                config.faults().latency());
        heal(catalog, "before start", out);
        var finished = new AtomicBoolean();
        var healOnExit = Thread.ofPlatform().unstarted(() -> {
            if (!finished.get()) {
                heal(catalog, "on exit", out);
            }
        });
        Runtime.getRuntime().addShutdownHook(healOnExit);

        try (var history = new JsonLog(config.runDir().resolve("history.jsonl"), JSON);
                var faults = new JsonLog(config.runDir().resolve("faults.jsonl"), JSON)) {
            var workload =
                    new Workload(new LedgerClient(targets.ledger(), JSON), history, config.load(), config.seed());
            out.printf(
                    "seed %d, scenario %s, %d workers for %s%n",
                    config.seed(),
                    config.faults().scenario().label(),
                    config.load().workers(),
                    GoDuration.format(config.duration()));
            workload.setup();
            workload.fund();

            var started = Instant.now();
            var deadline = started.plus(config.duration());
            var nemesis = new Nemesis(
                    catalog,
                    config.faults().scenario().faults(),
                    config.faults().interval(),
                    config.faults().duration(),
                    config.seed(),
                    faults);
            var nemesisThread = Thread.ofVirtual().start(() -> {
                try {
                    nemesis.run(deadline);
                } catch (InterruptedException _) {
                    Thread.currentThread().interrupt();
                }
            });
            workload.run(deadline);
            nemesisThread.join();

            heal(catalog, "after run", out);
            finished.set(true);
            writeRun(config, started);
        } finally {
            if (finished.get()) {
                Runtime.getRuntime().removeShutdownHook(healOnExit);
            }
        }
    }

    private static void heal(Map<String, Faults.Fault> catalog, String when, PrintStream out) {
        try {
            Faults.healAll(catalog);
        } catch (IOException e) {
            out.printf("heal %s: %s%n", when, e.getMessage());
        }
    }

    private static void writeRun(Config config, Instant started) {
        var run = new LinkedHashMap<String, Object>();
        run.put("seed", config.seed());
        run.put("scenario", config.faults().scenario().label());
        run.put("faults", config.faults().scenario().faults());
        run.put("accounts", config.load().accounts());
        run.put("workers", config.load().workers());
        run.put("duration", GoDuration.format(config.duration()));
        run.put("clientTimeout", GoDuration.format(config.load().clientTimeout()));
        run.put("fundingMinor", config.load().fundingMinor());
        run.put("amountMaxMinor", config.load().amountMaxMinor());
        run.put("startedAt", started.truncatedTo(ChronoUnit.SECONDS).toString());
        run.put("endedAt", Instant.now().truncatedTo(ChronoUnit.SECONDS).toString());
        JSON.writeValue(config.runDir().resolve("run.json").toFile(), run);
    }
}
