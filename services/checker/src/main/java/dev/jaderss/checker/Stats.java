package dev.jaderss.checker;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.stream.Stream;
import tools.jackson.databind.json.JsonMapper;

record Stats(
        double seconds,
        int keys,
        int attempts,
        int ok,
        int fail,
        int unknown,
        double okPerSecond,
        long p50Ms,
        long p99Ms,
        long maxMs,
        long commits,
        long rollbacks,
        List<Second> timeline,
        List<FaultWindow> faults) {

    record Second(int t, int ok, int fail, int unknown) {

        Second plus(String phase) {
            return switch (phase) {
                case History.OK -> new Second(t, ok + 1, fail, unknown);
                case History.FAIL -> new Second(t, ok, fail + 1, unknown);
                default -> new Second(t, ok, fail, unknown + 1);
            };
        }
    }

    record FaultWindow(String fault, double start, double end) {}

    record LogEvent(long t, int worker, String key, String phase, boolean mismatch, String fault) {}

    Stats withCounters(Snapshot.Counters counters) {
        return new Stats(
                seconds,
                keys,
                attempts,
                ok,
                fail,
                unknown,
                okPerSecond,
                p50Ms,
                p99Ms,
                maxMs,
                counters.commits(),
                counters.rollbacks(),
                timeline,
                faults);
    }

    static Stats read(Stream<String> history, Stream<String> faults, JsonMapper json) {
        var start = 0L;
        var end = 0L;
        int attempts = 0, ok = 0, fail = 0, unknown = 0;
        var keys = new HashSet<String>();
        var pending = new HashMap<String, Deque<Long>>();
        var latencies = new ArrayList<Long>();
        var buckets = new HashMap<Integer, Second>();
        for (var line : (Iterable<String>) history::iterator) {
            var e = json.readValue(line, LogEvent.class);
            if (e.worker() < 0) {
                continue;
            }
            if (start == 0) {
                start = e.t();
            }
            end = Math.max(end, e.t());
            var attempt = e.key() + "|" + e.worker() + "|" + e.mismatch();
            if (History.INVOKE.equals(e.phase())) {
                if (!e.mismatch()) {
                    keys.add(e.key());
                }
                pending.computeIfAbsent(attempt, _ -> new ArrayDeque<>()).addLast(e.t());
                continue;
            }
            attempts++;
            switch (e.phase()) {
                case History.OK -> ok++;
                case History.FAIL -> fail++;
                default -> unknown++;
            }
            var second = (int) ((e.t() - start) / 1000);
            buckets.merge(second, new Second(second, 0, 0, 0).plus(e.phase()), (a, _) -> a.plus(e.phase()));
            var invoked = pending.get(attempt);
            if (invoked != null && !invoked.isEmpty()) {
                var at = invoked.pollFirst();
                if (!History.UNKNOWN.equals(e.phase())) {
                    latencies.add(e.t() - at);
                }
            }
        }
        var seconds = (end - start) / 1000.0;
        latencies.sort(null);
        var timeline = new ArrayList<Second>();
        for (var t = 0; t <= (int) seconds; t++) {
            timeline.add(buckets.getOrDefault(t, new Second(t, 0, 0, 0)));
        }
        return new Stats(
                seconds,
                keys.size(),
                attempts,
                ok,
                fail,
                unknown,
                seconds > 0 ? ok / seconds : 0,
                percentile(latencies, 0.50),
                percentile(latencies, 0.99),
                latencies.isEmpty() ? 0 : latencies.getLast(),
                0,
                0,
                timeline,
                windows(faults, start, seconds, json));
    }

    private static List<FaultWindow> windows(Stream<String> faults, long start, double seconds, JsonMapper json) {
        var windows = new ArrayList<FaultWindow>();
        var open = new LinkedHashMap<String, Double>();
        for (var line : (Iterable<String>) faults::iterator) {
            var f = json.readValue(line, LogEvent.class);
            var at = (f.t() - start) / 1000.0;
            switch (f.phase()) {
                case "start" -> open.put(f.fault(), at);
                case "end" -> {
                    var from = open.remove(f.fault());
                    if (from != null) {
                        windows.add(new FaultWindow(f.fault(), from, at));
                    }
                }
                default -> {}
            }
        }
        open.forEach((fault, from) -> windows.add(new FaultWindow(fault, from, seconds)));
        return windows;
    }

    static long percentile(List<Long> sorted, double p) {
        if (sorted.isEmpty()) {
            return 0;
        }
        var i = (int) (p * sorted.size() + 0.999999) - 1;
        return sorted.get(Math.max(0, Math.min(i, sorted.size() - 1)));
    }
}
