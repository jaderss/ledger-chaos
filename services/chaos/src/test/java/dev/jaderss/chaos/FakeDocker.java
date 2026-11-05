package dev.jaderss.chaos;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.StandardProtocolFamily;
import java.net.URLDecoder;
import java.net.UnixDomainSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

final class FakeDocker implements AutoCloseable {

    private final Path socket;
    private final ServerSocketChannel server;
    private final List<String> actions = new CopyOnWriteArrayList<>();
    private final Thread acceptor;

    FakeDocker() throws IOException {
        socket = Files.createTempDirectory("dk").resolve("docker.sock");
        server = ServerSocketChannel.open(StandardProtocolFamily.UNIX);
        server.bind(UnixDomainSocketAddress.of(socket));
        acceptor = Thread.ofVirtual().start(this::accept);
    }

    Path socket() {
        return socket;
    }

    UnixDomainSocketAddress address() {
        return UnixDomainSocketAddress.of(socket);
    }

    List<String> actions() {
        return List.copyOf(actions);
    }

    private void accept() {
        while (server.isOpen()) {
            try {
                var channel = server.accept();
                Thread.ofVirtual().start(() -> serve(channel));
            } catch (IOException _) {
                return;
            }
        }
    }

    private void serve(SocketChannel channel) {
        try (channel) {
            var request = readHead(channel);
            var target = request.split(" ")[1];
            channel.write(
                    ByteBuffer.wrap(answer(request.startsWith("GET"), target).getBytes(StandardCharsets.UTF_8)));
        } catch (IOException _) {
        }
    }

    private String answer(boolean list, String target) {
        if (list) {
            var filters = URLDecoder.decode(target, StandardCharsets.UTF_8);
            return response(200, filters.contains("service=missing") ? "[]" : "[{\"Id\":\"c1\"}]");
        }
        actions.add(target);
        if (target.endsWith("/unpause")) {
            return response(500, "{\"message\":\"Container c1 is not paused\"}");
        }
        if (target.endsWith("/pause")) {
            return response(409, "");
        }
        return response(204, "");
    }

    private static String response(int status, String body) {
        return "HTTP/1.0 %d X\r\nContent-Type: application/json\r\nContent-Length: %d\r\n\r\n%s"
                .formatted(status, body.length(), body);
    }

    private static String readHead(SocketChannel channel) throws IOException {
        var head = new ByteArrayOutputStream();
        var buffer = ByteBuffer.allocate(1024);
        while (!head.toString(StandardCharsets.US_ASCII).contains("\r\n\r\n") && channel.read(buffer) > 0) {
            head.write(buffer.array(), 0, buffer.position());
            buffer.clear();
        }
        return head.toString(StandardCharsets.US_ASCII);
    }

    @Override
    public void close() throws IOException {
        server.close();
        acceptor.interrupt();
        Files.deleteIfExists(socket);
    }
}
