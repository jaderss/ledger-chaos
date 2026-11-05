package dev.jaderss.chaos;

import java.net.URI;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

record Config(Path runDir, Targets targets, Load load, Faults faults, Duration duration, long seed) {

    record Targets(URI ledger, URI toxiproxy, Path dockerSocket, String project) {}

    record Load(
            int accounts,
            int overdraftEvery,
            int workers,
            int retries,
            Duration clientTimeout,
            double duplicateRate,
            double mismatchRate,
            long fundingMinor,
            long amountMaxMinor) {}

    record Faults(Scenario scenario, Duration interval, Duration duration, Duration latency) {}

    static Config from(Map<String, String> env) {
        var reader = new Reader(env);
        var config = new Config(
                Path.of(reader.text("RUN_DIR", "/results/local")),
                new Targets(
                        URI.create(reader.text("LEDGER_URL", "http://ledger:8080")),
                        URI.create(reader.text("TOXIPROXY_URL", "http://toxiproxy:8474")),
                        Path.of(reader.text("DOCKER_SOCKET", "/var/run/docker.sock")),
                        reader.text("COMPOSE_PROJECT", "ledger-chaos")),
                new Load(
                        reader.parsed("ACCOUNTS", 20, Integer::parseInt),
                        reader.parsed("OVERDRAFT_EVERY", 10, Integer::parseInt),
                        reader.parsed("WORKERS", 64, Integer::parseInt),
                        reader.parsed("RETRIES", 5, Integer::parseInt),
                        reader.parsed("CLIENT_TIMEOUT", Duration.ofSeconds(1), GoDuration::parse),
                        reader.parsed("DUPLICATE_RATE", 0.05, Double::parseDouble),
                        reader.parsed("MISMATCH_RATE", 0.01, Double::parseDouble),
                        reader.parsed("FUNDING_MINOR", 100_000L, Long::parseLong),
                        reader.parsed("AMOUNT_MAX_MINOR", 5_000L, Long::parseLong)),
                new Faults(
                        reader.parsed("SCENARIO", Scenario.CONTENTION, Scenario::of),
                        reader.parsed("FAULT_INTERVAL", Duration.ofSeconds(10), GoDuration::parse),
                        reader.parsed("FAULT_DURATION", Duration.ofSeconds(3), GoDuration::parse),
                        reader.parsed("DB_LATENCY", Duration.ofMillis(1500), GoDuration::parse)),
                reader.parsed("DURATION", Duration.ofMinutes(2), GoDuration::parse),
                reader.parsed("SEED", seedFromClock(), Long::parseLong));
        var load = config.load();
        if (load.accounts() < 2 || load.workers() < 1 || load.amountMaxMinor() < 1) {
            reader.errors.add("ACCOUNTS needs at least 2, WORKERS and AMOUNT_MAX_MINOR at least 1");
        }
        if (!reader.errors.isEmpty()) {
            throw new IllegalArgumentException("config: " + String.join("; ", reader.errors));
        }
        return config;
    }

    private static long seedFromClock() {
        var now = Instant.now();
        return now.getEpochSecond() * 1_000_000_000L + now.getNano();
    }

    private static final class Reader {
        private final Map<String, String> env;
        private final List<String> errors = new ArrayList<>();

        Reader(Map<String, String> env) {
            this.env = env;
        }

        String text(String name, String fallback) {
            var value = env.get(name);
            return value == null || value.isEmpty() ? fallback : value;
        }

        <T> T parsed(String name, T fallback, Function<String, T> parse) {
            var value = env.get(name);
            if (value == null || value.isEmpty()) {
                return fallback;
            }
            try {
                return parse.apply(value);
            } catch (IllegalArgumentException e) {
                errors.add("%s: %s".formatted(name, e.getMessage()));
                return fallback;
            }
        }
    }
}
