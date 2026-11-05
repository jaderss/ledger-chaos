package dev.jaderss.chaos;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Map;
import java.util.TreeMap;

final class Faults {

    interface Action {
        void run() throws IOException;
    }

    record Fault(Action apply, Action heal) {}

    private Faults() {}

    static Map<String, Fault> catalog(Docker docker, Toxiproxy toxiproxy, Duration latency) {
        var catalog = new TreeMap<String, Fault>();
        for (var service : new String[] {"ledger", "relay", "projector"}) {
            catalog.put("kill-" + service, new Fault(() -> docker.kill(service), () -> docker.start(service)));
        }
        catalog.put("pause-db", new Fault(() -> docker.pause("postgres"), () -> docker.unpause("postgres")));
        catalog.put("restart-db", new Fault(() -> docker.restart("postgres"), () -> {}));
        catalog.put(
                "db-cut",
                new Fault(() -> toxiproxy.setEnabled("postgres", false), () -> toxiproxy.setEnabled("postgres", true)));
        catalog.put(
                "db-latency",
                new Fault(
                        () -> toxiproxy.addToxic(
                                "postgres", "latency", "latency", Map.of("latency", latency.toMillis())),
                        () -> toxiproxy.removeToxic("postgres", "latency")));
        catalog.put(
                "db-reset",
                new Fault(
                        () -> toxiproxy.addToxic("postgres", "reset", "reset_peer", Map.of("timeout", 0L)),
                        () -> toxiproxy.removeToxic("postgres", "reset")));
        catalog.put("kafka-down", new Fault(() -> docker.stop("kafka"), () -> docker.start("kafka")));
        return catalog;
    }

    static void healAll(Map<String, Fault> catalog) throws IOException {
        var errors = new ArrayList<String>();
        new TreeMap<>(catalog).forEach((name, fault) -> {
            try {
                fault.heal().run();
            } catch (IOException e) {
                errors.add(name + ": " + e.getMessage());
            }
        });
        if (!errors.isEmpty()) {
            throw new IOException(String.join("\n", errors));
        }
    }
}
