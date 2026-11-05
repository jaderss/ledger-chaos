package dev.jaderss.chaos;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.SocketAddress;
import java.net.URLEncoder;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.SocketChannel;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import tools.jackson.databind.json.JsonMapper;

final class Docker {

    record Response(int status, String body) {}

    record Container(@JsonProperty("Id") String id) {}

    private final SocketAddress socket;
    private final String project;
    private final JsonMapper json;

    Docker(SocketAddress socket, String project, JsonMapper json) {
        this.socket = socket;
        this.project = project;
        this.json = json;
    }

    void kill(String service) throws IOException {
        action(service, "kill?signal=SIGKILL");
    }

    void start(String service) throws IOException {
        action(service, "start");
    }

    void stop(String service) throws IOException {
        action(service, "stop?t=0");
    }

    void restart(String service) throws IOException {
        action(service, "restart?t=0");
    }

    void pause(String service) throws IOException {
        action(service, "pause");
    }

    void unpause(String service) throws IOException {
        action(service, "unpause", "is not paused");
    }

    private String container(String service) throws IOException {
        var filters = json.writeValueAsString(Map.of(
                "label", List.of("com.docker.compose.project=" + project, "com.docker.compose.service=" + service)));
        var response =
                call("GET", "/containers/json?all=1&filters=" + URLEncoder.encode(filters, StandardCharsets.UTF_8));
        if (response.status() != 200) {
            throw new IOException("list containers: status %d %s".formatted(response.status(), response.body()));
        }
        var containers = json.readValue(response.body(), Container[].class);
        if (containers.length == 0) {
            throw new IOException("no container for service " + service);
        }
        return containers[0].id();
    }

    private void action(String service, String action, String... alreadyDone) throws IOException {
        var response = call("POST", "/containers/" + container(service) + "/" + action);
        if (response.status() == 204 || response.status() == 304) {
            return;
        }
        for (var done : alreadyDone) {
            if (response.body().contains(done)) {
                return;
            }
        }
        throw new IOException("%s %s: status %d %s"
                .formatted(action, service, response.status(), response.body().strip()));
    }

    Response call(String method, String path) throws IOException {
        try (var channel = SocketChannel.open(socket);
                var in = Channels.newInputStream(channel)) {
            var request = "%s %s HTTP/1.0\r\nHost: docker\r\nContent-Length: 0\r\n\r\n".formatted(method, path);
            channel.write(ByteBuffer.wrap(request.getBytes(StandardCharsets.US_ASCII)));
            var raw = new ByteArrayOutputStream();
            in.transferTo(raw);
            return parse(raw.toString(StandardCharsets.UTF_8));
        }
    }

    private static Response parse(String raw) throws IOException {
        var headerEnd = raw.indexOf("\r\n\r\n");
        var statusLine = raw.lines().findFirst().orElse("");
        var parts = statusLine.split(" ", 3);
        if (headerEnd < 0 || parts.length < 2) {
            throw new IOException("malformed Docker response: " + statusLine);
        }
        return new Response(Integer.parseInt(parts[1]), raw.substring(headerEnd + 4));
    }
}
