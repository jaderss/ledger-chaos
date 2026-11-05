package dev.jaderss.checker;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.ArrayList;
import java.util.List;

record Result(
        String id,
        String rule,
        boolean pass,
        @JsonProperty("violations") int count,
        @JsonInclude(JsonInclude.Include.NON_EMPTY) List<String> examples) {

    static final class Violations {
        private static final int EXAMPLES = 5;

        private int count;
        private final List<String> examples = new ArrayList<>();

        void add(String format, Object... args) {
            count++;
            if (examples.size() < EXAMPLES) {
                examples.add(format.formatted(args));
            }
        }

        Result result(String id, String rule) {
            return new Result(id, rule, count == 0, count, List.copyOf(examples));
        }
    }
}
