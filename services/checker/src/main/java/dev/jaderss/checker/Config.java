package dev.jaderss.checker;

import java.net.URI;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Locale;
import java.util.Map;

record Config(
        Path runDir,
        Mode mode,
        String locking,
        Duration quietTimeout,
        Kafka kafka,
        Postgres ledger,
        Postgres projector) {

    record Kafka(String brokers, String topic, String deadLetterTopic, String group) {}

    record Postgres(String url, String user, String password) {

        static Postgres of(String dsn) {
            var uri = URI.create(dsn);
            var credentials = uri.getUserInfo().split(":", 2);
            var port = uri.getPort() < 0 ? 5432 : uri.getPort();
            return new Postgres(
                    "jdbc:postgresql://%s:%d%s".formatted(uri.getHost(), port, uri.getPath()),
                    credentials[0],
                    credentials.length > 1 ? credentials[1] : "");
        }
    }

    static Config from(Map<String, String> env) {
        var mode = Mode.of(get(env, "LEDGER_MODE", "correct"));
        return new Config(
                Path.of(get(env, "RUN_DIR", "/results/local")),
                mode,
                get(env, "LEDGER_LOCKING", "ordered"),
                duration(get(env, "QUIET_TIMEOUT", "60s")),
                new Kafka(
                        get(env, "KAFKA_BROKERS", "kafka:29092"),
                        get(env, "TOPIC", "ledger.transfers"),
                        get(env, "DLT_TOPIC", "ledger.transfers.DLT"),
                        get(env, "GROUP", "projector")),
                Postgres.of(get(env, "LEDGER_DSN", "postgres://postgres:postgres@postgres:5432/ledger")),
                Postgres.of(get(env, "PROJECTOR_DSN", "postgres://postgres:postgres@postgres:5432/projector")));
    }

    private static String get(Map<String, String> env, String name, String fallback) {
        var value = env.get(name);
        return value == null || value.isEmpty() ? fallback : value;
    }

    static Duration duration(String value) {
        try {
            return Duration.parse("PT" + value.toUpperCase(Locale.ROOT));
        } catch (RuntimeException e) {
            throw new IllegalArgumentException("QUIET_TIMEOUT: cannot parse \"%s\"".formatted(value), e);
        }
    }
}
