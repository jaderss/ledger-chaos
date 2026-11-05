package dev.jaderss.chaos;

import dev.jaderss.chaos.Faults.Action;
import dev.jaderss.chaos.Faults.Fault;
import dev.jaderss.chaos.JsonLog.FaultEntry;
import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.SplittableRandom;

final class Nemesis {

    private final Map<String, Fault> catalog;
    private final List<String> faults;
    private final Duration interval;
    private final Duration duration;
    private final SplittableRandom random;
    private final JsonLog log;

    Nemesis(
            Map<String, Fault> catalog,
            List<String> faults,
            Duration interval,
            Duration duration,
            long seed,
            JsonLog log) {
        this.catalog = catalog;
        this.faults = faults;
        this.interval = interval;
        this.duration = duration;
        this.random = new SplittableRandom(seed);
        this.log = log;
    }

    void run(Instant deadline) throws InterruptedException {
        if (faults.isEmpty()) {
            sleepUntil(deadline);
            return;
        }
        var next = Instant.now().plus(interval);
        while (next.isBefore(deadline)) {
            sleepUntil(next);
            inject(faults.get(random.nextInt(faults.size())), deadline);
            var now = Instant.now();
            while (!next.isAfter(now)) {
                next = next.plus(interval);
            }
        }
        sleepUntil(deadline);
    }

    private void inject(String name, Instant deadline) throws InterruptedException {
        var fault = catalog.get(name);
        log.write(FaultEntry.of(name, "start", null));
        attempt(name, fault.apply());
        var heal = Instant.now().plus(duration);
        sleepUntil(heal.isBefore(deadline) ? heal : deadline);
        attempt(name, fault.heal());
        log.write(FaultEntry.of(name, "end", null));
    }

    private void attempt(String name, Action action) {
        try {
            action.run();
        } catch (IOException | RuntimeException e) {
            log.write(FaultEntry.of(name, "error", String.valueOf(e.getMessage())));
        }
    }

    private static void sleepUntil(Instant at) throws InterruptedException {
        var wait = Duration.between(Instant.now(), at);
        if (wait.isPositive()) {
            Thread.sleep(wait);
        }
    }
}
