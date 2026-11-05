package dev.jaderss.ledger.api;

import dev.jaderss.ledger.PostgresContainer;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.SpringBootTest.WebEnvironment;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.context.annotation.Import;
import org.springframework.test.web.servlet.client.RestTestClient;
import org.springframework.test.web.servlet.client.RestTestClient.ResponseSpec;

@SpringBootTest(webEnvironment = WebEnvironment.RANDOM_PORT)
@Import(PostgresContainer.class)
class AccountControllerTest {

    @LocalServerPort
    int port;

    RestTestClient client;

    @BeforeEach
    void setUp() {
        client = RestTestClient.bindToServer()
                .baseUrl("http://localhost:" + port)
                .build();
    }

    private ResponseSpec create(UUID id, String currency, boolean overdraft) {
        return client.post()
                .uri("/accounts")
                .body(Map.of("id", id, "currency", currency, "allowOverdraft", overdraft))
                .exchange();
    }

    @Test
    @DisplayName("a new account starts at zero, and creating it again with the same settings is a 200")
    void createIsIdempotent() {
        var id = UUID.randomUUID();
        create(id, "EUR", false)
                .expectStatus()
                .isCreated()
                .expectBody()
                .jsonPath("$.balanceMinor")
                .isEqualTo(0);
        create(id, "EUR", false).expectStatus().isOk();
    }

    @Test
    @DisplayName("creating an existing account with other settings is a 409")
    void conflict() {
        var id = UUID.randomUUID();
        create(id, "EUR", false).expectStatus().isCreated();
        create(id, "EUR", true)
                .expectStatus()
                .isEqualTo(409)
                .expectBody()
                .jsonPath("$.type")
                .isEqualTo("/problems/account-conflict");
    }

    @Test
    @DisplayName("an unknown account is a 404")
    void unknown() {
        client.get()
                .uri("/accounts/" + UUID.randomUUID())
                .exchange()
                .expectStatus()
                .isNotFound();
    }
}
