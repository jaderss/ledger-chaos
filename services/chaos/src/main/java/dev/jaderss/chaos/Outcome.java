package dev.jaderss.chaos;

import tools.jackson.core.JacksonException;
import tools.jackson.databind.json.JsonMapper;

record Outcome(
        String phase,
        int status,
        String transferId,
        String transferStatus,
        String problem,
        boolean replayed,
        String error) {

    static final String OK = "ok";
    static final String FAIL = "fail";
    static final String UNKNOWN = "unknown";

    record Answer(String id, String status, String type, String transferId, String transferStatus) {

        static final Answer EMPTY = new Answer(null, null, null, null, null);

        String problem() {
            return type == null || type.isEmpty() ? null : type.substring(type.lastIndexOf('/') + 1);
        }
    }

    static Outcome failure(String error) {
        return new Outcome(UNKNOWN, 0, null, null, null, false, error);
    }

    static Outcome classify(int status, boolean replayed, byte[] body, JsonMapper json) {
        var answer = parse(body, json);
        if (status == 200 || status == 201) {
            return new Outcome(OK, status, answer.id(), answer.status(), null, replayed, null);
        }
        if (status >= 400 && status < 500) {
            return new Outcome(
                    FAIL, status, answer.transferId(), answer.transferStatus(), answer.problem(), replayed, null);
        }
        return new Outcome(UNKNOWN, status, null, null, answer.problem(), replayed, null);
    }

    private static Answer parse(byte[] body, JsonMapper json) {
        try {
            return json.readValue(body, Answer.class);
        } catch (JacksonException _) {
            return Answer.EMPTY;
        }
    }

    boolean definite() {
        return !UNKNOWN.equals(phase);
    }

    boolean ok() {
        return OK.equals(phase);
    }
}
