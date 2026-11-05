package dev.jaderss.chaos;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.params.provider.Arguments.argumentSet;

import java.nio.charset.StandardCharsets;
import java.util.stream.Stream;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class OutcomeTest {

    static Stream<Arguments> answers() {
        return Stream.of(
                argumentSet(
                        "created",
                        201,
                        false,
                        "{\"id\":\"t\",\"status\":\"POSTED\"}",
                        new Outcome("ok", 201, "t", "POSTED", null, false, null)),
                argumentSet(
                        "replayed",
                        200,
                        true,
                        "{\"id\":\"t\",\"status\":\"POSTED\"}",
                        new Outcome("ok", 200, "t", "POSTED", null, true, null)),
                argumentSet(
                        "rejected",
                        422,
                        false,
                        "{\"type\":\"/problems/insufficient-funds\",\"transferId\":\"t\",\"transferStatus\":\"REJECTED\"}",
                        new Outcome("fail", 422, "t", "REJECTED", "insufficient-funds", false, null)),
                argumentSet(
                        "not found",
                        404,
                        false,
                        "{\"type\":\"/problems/account-not-found\"}",
                        new Outcome("fail", 404, null, null, "account-not-found", false, null)),
                argumentSet(
                        "unavailable",
                        503,
                        false,
                        "{\"type\":\"/problems/unavailable\"}",
                        new Outcome("unknown", 503, null, null, "unavailable", false, null)),
                argumentSet(
                        "not json", 502, false, "<html>", new Outcome("unknown", 502, null, null, null, false, null)));
    }

    @ParameterizedTest
    @MethodSource("answers")
    @DisplayName("a status and body classify into ok, fail or unknown")
    void classifies(int status, boolean replayed, String body, Outcome expected) {
        assertThat(Outcome.classify(status, replayed, body.getBytes(StandardCharsets.UTF_8), Chaos.json()))
                .isEqualTo(expected);
    }
}
