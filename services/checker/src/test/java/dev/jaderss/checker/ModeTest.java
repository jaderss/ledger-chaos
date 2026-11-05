package dev.jaderss.checker;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

class ModeTest {

    private static List<Result> failing(String failing) {
        var ids = failing.isBlank() ? List.<String>of() : Arrays.asList(failing.split(" "));
        return Stream.of("I1", "I2", "I3", "I4", "I5", "I6", "I7", "I8", "I9", "I11")
                .map(id -> new Result(id, "", !ids.contains(id), 0, List.of()))
                .toList();
    }

    @ParameterizedTest
    @CsvSource(delimiter = '|', textBlock = """
                    correct           |             | true  | correct passes every invariant
                    correct           | I9          | false | correct fails I9
                    no-idempotency    | I4 I6 I7    | true  | no-idempotency fails I4, I6, I7
                    no-idempotency    | I4 I7       | false | passes I6
                    dual-write        | I8 I9       | true  | dual-write fails I8, I9
                    dual-write        |             | false | must fail I8, I9 but passes I8, I9
                    unordered-locks   | I11         | true  | unordered-locks fails I11
                    read-modify-write | I2 I3       | true  | read-modify-write fails I2, I3, and must fail I2
                    read-modify-write | I3          | false | passes I2
                    """)
    @DisplayName("each mode passes only when it fails at least its expected invariants")
    void verdict(String mode, String failed, boolean ok, String says) {
        var verdict = Mode.of(mode).verdict(failing(failed == null ? "" : failed));

        assertThat(verdict.ok()).isEqualTo(ok);
        assertThat(verdict.text()).contains(says);
    }

    @Test
    @DisplayName("every mode parses from its label, a typo does not")
    void parsesLabels() {
        assertThat(Mode.values()).allSatisfy(m -> assertThat(Mode.of(m.label())).isEqualTo(m));
        assertThatThrownBy(() -> Mode.of("corect")).hasMessageContaining("corect");
    }
}
