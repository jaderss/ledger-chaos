package dev.jaderss.chaos;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.Map;
import java.util.Set;
import tools.jackson.databind.json.JsonMapper;

final class Toxiproxy {

    private static final Duration TIMEOUT = Duration.ofSeconds(30);

    private final URI base;
    private final HttpClient http =
            HttpClient.newBuilder().version(HttpClient.Version.HTTP_1_1).build();
    private final JsonMapper json;

    Toxiproxy(URI base, JsonMapper json) {
        this.base = base;
        this.json = json;
    }

    void setEnabled(String proxy, boolean enabled) throws IOException {
        call("POST", "/proxies/" + proxy, Map.of("enabled", enabled), Set.of(200));
    }

    void addToxic(String proxy, String name, String type, Map<String, Long> attributes) throws IOException {
        var toxic = Map.of("name", name, "type", type, "stream", "downstream", "attributes", attributes);
        call("POST", "/proxies/" + proxy + "/toxics", toxic, Set.of(200));
    }

    void removeToxic(String proxy, String name) throws IOException {
        call("DELETE", "/proxies/" + proxy + "/toxics/" + name, null, Set.of(204, 404));
    }

    private void call(String method, String path, Object body, Set<Integer> ok) throws IOException {
        var publisher = body == null
                ? HttpRequest.BodyPublishers.noBody()
                : HttpRequest.BodyPublishers.ofByteArray(json.writeValueAsBytes(body));
        var request = HttpRequest.newBuilder(base.resolve(path))
                .timeout(TIMEOUT)
                .method(method, publisher)
                .build();
        try {
            var status =
                    http.send(request, HttpResponse.BodyHandlers.discarding()).statusCode();
            if (!ok.contains(status)) {
                throw new IOException("%s %s: status %d".formatted(method, path, status));
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("interrupted", e);
        }
    }
}
