package dev.jaderss.checker;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import tools.jackson.core.JacksonException;
import tools.jackson.databind.json.JsonMapper;

final class History {

    record Body(String from, String to, long amount, String currency) {}

    record Answer(
            String phase, int status, String transferId, String transferStatus, String problem, boolean replayed) {

        static final Answer NONE = new Answer(UNKNOWN, 0, null, null, null, false);

        boolean definite() {
            return !UNKNOWN.equals(phase);
        }
    }

    record MismatchAttempt(Body body, Answer answer) {}

    record KeyHistory(String key, Body body, List<Answer> answers, List<MismatchAttempt> mismatches) {

        List<Answer> definite() {
            return answers.stream().filter(Answer::definite).toList();
        }

        boolean posted() {
            return answers.stream().anyMatch(a -> OK.equals(a.phase()));
        }
    }

    record Entry(
            String op,
            String key,
            String phase,
            Body body,
            boolean mismatch,
            int status,
            String transferId,
            String transferStatus,
            String problem,
            boolean replayed) {

        Answer answer() {
            return new Answer(phase, status, transferId, transferStatus, problem, replayed);
        }
    }

    static final String INVOKE = "invoke";
    static final String OK = "ok";
    static final String FAIL = "fail";
    static final String UNKNOWN = "unknown";

    private History() {}

    static Map<String, KeyHistory> read(Stream<String> lines, JsonMapper json) {
        var keys = new HashMap<String, Builder>();
        var lineNumber = 0;
        for (var line : (Iterable<String>) lines::iterator) {
            lineNumber++;
            var entry = parse(json, line, lineNumber);
            if (!"transfer".equals(entry.op())) {
                continue;
            }
            var key = keys.computeIfAbsent(entry.key(), Builder::new);
            if (!key.add(entry)) {
                throw new IllegalArgumentException(
                        "history line %d: mismatch result without invoke".formatted(lineNumber));
            }
        }
        var histories = new HashMap<String, KeyHistory>();
        keys.forEach((name, builder) -> histories.put(name, builder.build()));
        return histories;
    }

    private static Entry parse(JsonMapper json, String line, int lineNumber) {
        try {
            return json.readValue(line, Entry.class);
        } catch (JacksonException e) {
            throw new IllegalArgumentException("history line %d: %s".formatted(lineNumber, e.getOriginalMessage()), e);
        }
    }

    private static final class Builder {
        private final String key;
        private Body body;
        private final List<Answer> answers = new ArrayList<>();
        private final List<Body> mismatchBodies = new ArrayList<>();
        private final List<Answer> mismatchAnswers = new ArrayList<>();
        private final Deque<Integer> pendingMismatches = new ArrayDeque<>();

        Builder(String key) {
            this.key = key;
        }

        boolean add(Entry entry) {
            var invoke = INVOKE.equals(entry.phase());
            if (invoke && entry.mismatch()) {
                pendingMismatches.addLast(mismatchBodies.size());
                mismatchBodies.add(entry.body());
                mismatchAnswers.add(Answer.NONE);
            } else if (invoke) {
                if (body == null) {
                    body = entry.body();
                }
            } else if (entry.mismatch()) {
                var index = pendingMismatches.pollFirst();
                if (index == null) {
                    return false;
                }
                mismatchAnswers.set(index, entry.answer());
            } else {
                answers.add(entry.answer());
            }
            return true;
        }

        KeyHistory build() {
            var mismatches = new ArrayList<MismatchAttempt>();
            for (var i = 0; i < mismatchBodies.size(); i++) {
                mismatches.add(new MismatchAttempt(mismatchBodies.get(i), mismatchAnswers.get(i)));
            }
            return new KeyHistory(key, body, List.copyOf(answers), List.copyOf(mismatches));
        }
    }
}
