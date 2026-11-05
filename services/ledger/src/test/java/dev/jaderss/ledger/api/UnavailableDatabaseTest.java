package dev.jaderss.ledger.api;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import dev.jaderss.ledger.Fixtures;
import dev.jaderss.ledger.domain.AccountId;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpRequest.BodyPublishers;
import java.net.http.HttpResponse.BodyHandlers;
import java.time.Duration;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.SpringBootTest.WebEnvironment;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.context.annotation.Import;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.web.servlet.client.RestTestClient;
import org.springframework.test.web.servlet.client.RestTestClient.ResponseSpec;
import org.testcontainers.containers.Network;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;
import org.testcontainers.toxiproxy.ToxiproxyContainer;

@Testcontainers
@SpringBootTest(webEnvironment = WebEnvironment.RANDOM_PORT)
@Import(Fixtures.class)
class UnavailableDatabaseTest {

    private static final Network NETWORK = Network.newNetwork();
    private static final int PROXY_PORT = 8666;
    private static final HttpClient HTTP = HttpClient.newHttpClient();

    @Container
    static final PostgreSQLContainer POSTGRES =
            new PostgreSQLContainer("postgres:18.6").withNetwork(NETWORK).withNetworkAliases("postgres");

    @Container
    static final ToxiproxyContainer TOXIPROXY =
            new ToxiproxyContainer("ghcr.io/shopify/toxiproxy:2.13.0").withNetwork(NETWORK);

    @LocalServerPort
    int port;

    @Autowired
    Fixtures fixtures;

    @DynamicPropertySource
    static void datasourceThroughProxy(DynamicPropertyRegistry registry) throws Exception {
        toxiproxy("/proxies", """
                {"name": "postgres", "listen": "0.0.0.0:%d", "upstream": "postgres:5432"}
                """.formatted(PROXY_PORT));
        registry.add(
                "spring.datasource.url",
                () -> "jdbc:postgresql://%s:%d/%s?socketTimeout=2"
                        .formatted(
                                TOXIPROXY.getHost(), TOXIPROXY.getMappedPort(PROXY_PORT), POSTGRES.getDatabaseName()));
        registry.add("spring.datasource.username", POSTGRES::getUsername);
        registry.add("spring.datasource.password", POSTGRES::getPassword);
        registry.add("spring.datasource.hikari.connection-timeout", () -> "1000");
    }

    private static void toxiproxy(String path, String json) throws IOException, InterruptedException {
        var uri = URI.create("http://%s:%d%s".formatted(TOXIPROXY.getHost(), TOXIPROXY.getMappedPort(8474), path));
        var response = HTTP.send(
                HttpRequest.newBuilder(uri).POST(BodyPublishers.ofString(json)).build(), BodyHandlers.ofString());
        assertThat(response.statusCode()).as(response.body()).isBetween(200, 299);
    }

    private static void database(boolean enabled) throws IOException, InterruptedException {
        toxiproxy("/proxies/postgres", "{\"enabled\": %b}".formatted(enabled));
    }

    private ResponseSpec post(String key, AccountId from, AccountId to) {
        return RestTestClient.bindToServer()
                .baseUrl("http://localhost:" + port)
                .build()
                .post()
                .uri("/transfers")
                .header("Idempotency-Key", key)
                .body(Map.of(
                        "fromAccount", from.value(), "toAccount", to.value(), "amountMinor", 100, "currency", "EUR"))
                .exchange();
    }

    @Test
    @DisplayName("a cut database is a 503 with outcome unknown, and retries with the same key post once")
    void cutDatabase() throws Exception {
        var from = fixtures.account(1_000);
        var to = fixtures.account(0);
        var key = "k-" + UUID.randomUUID();

        database(false);
        try {
            post(key, from, to)
                    .expectStatus()
                    .isEqualTo(503)
                    .expectBody()
                    .jsonPath("$.type")
                    .isEqualTo("/problems/unavailable");
        } finally {
            database(true);
        }

        var afterHeal = await().atMost(Duration.ofSeconds(15))
                .until(
                        () -> post(key, from, to).returnResult(String.class).getStatus(),
                        status -> status.value() != 503);

        assertThat(afterHeal.value()).isIn(200, 201);
        post(key, from, to).expectStatus().isOk();
        assertThat(fixtures.balance(from)).isEqualTo(900);
    }
}
