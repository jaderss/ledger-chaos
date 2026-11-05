package dev.jaderss.checker;

import java.util.Arrays;
import java.util.List;

enum Mode {
    CORRECT("correct"),
    NO_IDEMPOTENCY("no-idempotency", "I4", "I6", "I7"),
    DUAL_WRITE("dual-write", "I8", "I9"),
    UNORDERED_LOCKS("unordered-locks", "I11"),
    READ_MODIFY_WRITE("read-modify-write", "I2");

    private final String label;
    private final List<String> mustFail;

    Mode(String label, String... mustFail) {
        this.label = label;
        this.mustFail = List.of(mustFail);
    }

    static Mode of(String label) {
        return Arrays.stream(values())
                .filter(m -> m.label.equals(label))
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException("unknown LEDGER_MODE \"%s\"".formatted(label)));
    }

    String label() {
        return label;
    }

    List<String> mustFail() {
        return mustFail;
    }

    Verdict verdict(List<Result> results) {
        var failed = results.stream().filter(r -> !r.pass()).map(Result::id).toList();
        if (mustFail.isEmpty()) {
            return failed.isEmpty()
                    ? new Verdict(true, label + " passes every invariant")
                    : new Verdict(false, label + " fails " + String.join(", ", failed));
        }
        var missing = mustFail.stream().filter(id -> !failed.contains(id)).toList();
        if (!missing.isEmpty()) {
            return new Verdict(
                    false,
                    "%s must fail %s but passes %s"
                            .formatted(label, String.join(", ", mustFail), String.join(", ", missing)));
        }
        return new Verdict(
                true,
                "%s fails %s, and must fail %s"
                        .formatted(label, String.join(", ", failed), String.join(", ", mustFail)));
    }

    record Verdict(boolean ok, String text) {}
}
