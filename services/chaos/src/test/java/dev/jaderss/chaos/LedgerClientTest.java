package dev.jaderss.chaos;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.equalTo;
import static com.github.tomakehurst.wiremock.client.WireMock.equalToJson;
import static com.github.tomakehurst.wiremock.client.WireMock.post;
import static com.github.tomakehurst.wiremock.client.WireMock.postRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.github.tomakehurst.wiremock.http.Fault;
import com.github.tomakehurst.wiremock.junit5.WireMockExtension;
import com.sun.net.httpserver.HttpServer;
import dev.jaderss.chaos.JsonLog.Body;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.time.Duration;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

class LedgerClientTest {

    @RegisterExtension
    static final WireMockExtension LEDGER = WireMockExtension.newInstance()
            .options(wireMockConfig().dynamicPort())
            .build();

    private static final Body BODY = new Body("a", "b", 1, "EUR");
    private static final Duration TIMEOUT = Duration.ofMillis(500);

    private LedgerClient client() {
        return new LedgerClient(URI.create(LEDGER.baseUrl()), Chaos.json());
    }

    @Test
    @DisplayName("a transfer sends the key twice, the body as JSON, and reads the replay header")
    void sendsKeyAndBody() {
        LEDGER.stubFor(post("/transfers")
                .willReturn(aResponse()
                        .withStatus(200)
                        .withHeader("Idempotent-Replayed", "true")
                        .withBody("{\"id\":\"t\",\"status\":\"POSTED\"}")));

        var outcome = client().transfer("k", BODY, TIMEOUT);

        assertThat(outcome).isEqualTo(new Outcome("ok", 200, "t", "POSTED", null, true, null));
        LEDGER.verify(postRequestedFor(urlEqualTo("/transfers"))
                .withHeader("Idempotency-Key", equalTo("k"))
                .withHeader("X-Request-Id", equalTo("k"))
                .withRequestBody(equalToJson(
                        "{\"fromAccount\":\"a\",\"toAccount\":\"b\",\"amountMinor\":1,\"currency\":\"EUR\"}")));
    }

    @ParameterizedTest
    @EnumSource(
            value = Fault.class,
            names = {"CONNECTION_RESET_BY_PEER", "EMPTY_RESPONSE", "RANDOM_DATA_THEN_CLOSE"})
    @DisplayName("a broken connection is an unknown outcome with the error kept")
    void brokenConnectionIsUnknown(Fault fault) {
        LEDGER.stubFor(post("/transfers").willReturn(aResponse().withFault(fault)));

        var outcome = client().transfer("k", BODY, TIMEOUT);

        assertThat(outcome.phase()).isEqualTo("unknown");
        assertThat(outcome.error()).isNotBlank();
    }

    @Test
    @DisplayName("an answer slower than the client timeout is an unknown outcome")
    void slowAnswerIsUnknown() {
        LEDGER.stubFor(post("/transfers")
                .willReturn(aResponse().withStatus(201).withFixedDelay(2_000).withBody("{}")));

        var outcome = client().transfer("k", BODY, TIMEOUT);

        assertThat(outcome.phase()).isEqualTo("unknown");
        assertThat(outcome.error()).contains("timed out");
    }

    @Test
    @DisplayName("account creation fails on anything but 200 or 201")
    void createAccountFailsOnConflict() {
        LEDGER.stubFor(post("/accounts").willReturn(aResponse().withStatus(409)));

        assertThatThrownBy(() -> client().createAccount("a", "EUR", false))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("409");
    }

    @Test
    @DisplayName("many parallel transfers reuse a small number of connections")
    void reusesConnections() throws Exception {
        var ports = ConcurrentHashMap.<Integer>newKeySet();
        var server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.setExecutor(Executors.newVirtualThreadPerTaskExecutor());
        server.createContext("/", exchange -> {
            ports.add(exchange.getRemoteAddress().getPort());
            exchange.getRequestBody().readAllBytes();
            var body = "{\"id\":\"t\",\"status\":\"POSTED\"}".getBytes();
            exchange.sendResponseHeaders(201, body.length);
            exchange.getResponseBody().write(body);
            exchange.close();
        });
        server.start();
        try {
            var client = new LedgerClient(
                    URI.create("http://127.0.0.1:" + server.getAddress().getPort()), Chaos.json());
            var workers = 16;
            try (var pool = Executors.newVirtualThreadPerTaskExecutor()) {
                for (var w = 0; w < workers; w++) {
                    var worker = w;
                    pool.submit(() -> {
                        for (var i = 0; i < 50; i++) {
                            client.transfer("k" + worker + "-" + i, BODY, Duration.ofSeconds(5));
                        }
                    });
                }
            }
            assertThat(ports).hasSizeLessThanOrEqualTo(2 * workers);
        } finally {
            server.stop(0);
        }
    }
}
