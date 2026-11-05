package dev.jaderss.chaos;

import dev.jaderss.chaos.JsonLog.Body;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.Map;
import tools.jackson.databind.json.JsonMapper;

final class LedgerClient {

    private static final String APPLICATION_JSON = "application/json";

    private final URI base;
    private final HttpClient http;
    private final JsonMapper json;

    LedgerClient(URI base, JsonMapper json) {
        this.base = base;
        this.json = json;
        this.http = HttpClient.newBuilder().version(HttpClient.Version.HTTP_1_1).build();
    }

    void createAccount(String id, String currency, boolean overdraft) throws IOException, InterruptedException {
        var body = json.writeValueAsBytes(Map.of("id", id, "currency", currency, "allowOverdraft", overdraft));
        var request = HttpRequest.newBuilder(base.resolve("/accounts"))
                .header("Content-Type", APPLICATION_JSON)
                .POST(HttpRequest.BodyPublishers.ofByteArray(body))
                .build();
        var status = http.send(request, HttpResponse.BodyHandlers.discarding()).statusCode();
        if (status != 201 && status != 200) {
            throw new IOException("create account %s: status %d".formatted(id, status));
        }
    }

    Outcome transfer(String key, Body body, Duration timeout) {
        var payload = json.writeValueAsBytes(Map.of(
                "fromAccount", body.from(),
                "toAccount", body.to(),
                "amountMinor", body.amount(),
                "currency", body.currency()));
        var request = HttpRequest.newBuilder(base.resolve("/transfers"))
                .timeout(timeout)
                .header("Content-Type", APPLICATION_JSON)
                .header("Idempotency-Key", key)
                .header("X-Request-Id", key)
                .POST(HttpRequest.BodyPublishers.ofByteArray(payload))
                .build();
        try {
            var response = http.send(request, HttpResponse.BodyHandlers.ofByteArray());
            var replayed = response.headers()
                    .firstValue("Idempotent-Replayed")
                    .map("true"::equals)
                    .orElse(false);
            return Outcome.classify(response.statusCode(), replayed, response.body(), json);
        } catch (IOException e) {
            return Outcome.failure(describe(e));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return Outcome.failure("interrupted");
        }
    }

    private static String describe(IOException e) {
        var message = e.getMessage();
        return message == null || message.isEmpty() ? e.getClass().getSimpleName() : message;
    }
}
