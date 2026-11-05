package dev.jaderss.chaos;

import java.util.Arrays;
import java.util.List;

enum Scenario {
    CONTENTION("contention"),
    DB_LATENCY("db-latency", "db-latency"),
    KAFKA_DOWN("kafka-down", "kafka-down"),
    KILLS("kills", "kill-ledger", "kill-relay", "kill-projector"),
    DB_FAULTS("db-faults", "pause-db", "restart-db", "db-cut", "db-reset"),
    ALL(
            "all",
            "kill-ledger",
            "kill-relay",
            "kill-projector",
            "pause-db",
            "restart-db",
            "db-cut",
            "db-latency",
            "db-reset",
            "kafka-down");

    private final String label;
    private final List<String> faults;

    Scenario(String label, String... faults) {
        this.label = label;
        this.faults = List.of(faults);
    }

    static Scenario of(String label) {
        return Arrays.stream(values())
                .filter(s -> s.label.equals(label))
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException("\"%s\" is not one of %s"
                        .formatted(
                                label,
                                Arrays.stream(values())
                                        .map(Scenario::label)
                                        .sorted()
                                        .toList())));
    }

    String label() {
        return label;
    }

    List<String> faults() {
        return faults;
    }
}
