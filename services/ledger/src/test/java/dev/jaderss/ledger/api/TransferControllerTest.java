package dev.jaderss.ledger.api;

import static org.assertj.core.api.Assertions.assertThat;

import dev.jaderss.ledger.Fixtures;
import dev.jaderss.ledger.PostgresContainer;
import dev.jaderss.ledger.domain.AccountId;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.SpringBootTest.WebEnvironment;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.context.annotation.Import;
import org.springframework.http.HttpStatus;
import org.springframework.test.web.servlet.client.RestTestClient;
import org.springframework.test.web.servlet.client.RestTestClient.ResponseSpec;

@SpringBootTest(webEnvironment = WebEnvironment.RANDOM_PORT)
@Import({PostgresContainer.class, Fixtures.class})
class TransferControllerTest {

    @LocalServerPort
    int port;

    @Autowired
    Fixtures fixtures;

    RestTestClient client;
    AccountId alice;
    AccountId bob;

    @BeforeEach
    void setUp() {
        client = RestTestClient.bindToServer()
                .baseUrl("http://localhost:" + port)
                .build();
        alice = fixtures.account(1_000);
        bob = fixtures.account(0);
    }

    private Map<String, Object> body(AccountId from, AccountId to, long amount, String currency) {
        return Map.of(
                "fromAccount", from.value(), "toAccount", to.value(), "amountMinor", amount, "currency", currency);
    }

    private ResponseSpec post(String key, Map<String, Object> body) {
        var request =
                client.post().uri("/transfers").header("X-Request-Id", "req-1").body(body);
        if (key != null) {
            request = request.header("Idempotency-Key", key);
        }
        return request.exchange();
    }

    private int transfersWithKey(String key) {
        return fixtures.sql(h -> h.createQuery("select count(*) from transfer where request_key = :key")
                .bind("key", key)
                .mapTo(Integer.class)
                .one());
    }

    @Test
    @DisplayName("a new transfer is posted with 201, a location and the correlation id")
    void posted() {
        post("k-" + UUID.randomUUID(), body(alice, bob, 400, "EUR"))
                .expectStatus()
                .isCreated()
                .expectHeader()
                .exists("Location")
                .expectHeader()
                .valueEquals("X-Request-Id", "req-1")
                .expectBody()
                .jsonPath("$.status")
                .isEqualTo("POSTED")
                .jsonPath("$.correlationId")
                .isEqualTo("req-1");

        assertThat(fixtures.balance(alice)).isEqualTo(600);
        assertThat(fixtures.balance(bob)).isEqualTo(400);
    }

    @Test
    @DisplayName("a posted transfer writes one outbox row whose event matches the transfer")
    void outbox() {
        var id = post("k-" + UUID.randomUUID(), body(alice, bob, 400, "EUR"))
                .expectStatus()
                .isCreated()
                .returnResult(TransferResponse.class)
                .getResponseBody()
                .id();

        var row = fixtures.sql(h -> h.createQuery("""
                select id, payload ->> 'eventId' as event_id, payload ->> 'currency' as currency,
                       (select sum((p ->> 'amountMinor')::bigint) from jsonb_array_elements(payload -> 'postings') p) as sum
                  from outbox where transfer_id = :id
                """)
                .bind("id", id)
                .map((rs, _) -> Map.of(
                        "id", rs.getString("id"),
                        "eventId", rs.getString("event_id"),
                        "currency", rs.getString("currency"),
                        "sum", rs.getLong("sum")))
                .one());
        assertThat(row)
                .containsEntry("eventId", row.get("id"))
                .containsEntry("currency", "EUR")
                .containsEntry("sum", 0L);
    }

    @Test
    @DisplayName("the same key and body replays the posted transfer with 200 and does not post again")
    void replayPosted() {
        var key = "k-" + UUID.randomUUID();
        var first = post(key, body(alice, bob, 400, "EUR"))
                .expectStatus()
                .isCreated()
                .returnResult(TransferResponse.class)
                .getResponseBody();

        post(key, body(alice, bob, 400, "EUR"))
                .expectStatus()
                .isOk()
                .expectHeader()
                .valueEquals(TransferController.REPLAYED, "true")
                .expectBody()
                .jsonPath("$.id")
                .isEqualTo(first.id().toString());

        assertThat(fixtures.balance(alice)).isEqualTo(600);
        assertThat(transfersWithKey(key)).isOne();
    }

    @Test
    @DisplayName("insufficient funds is a 422 stored as REJECTED")
    void insufficientFunds() {
        var key = "k-" + UUID.randomUUID();
        post(key, body(bob, alice, 1, "EUR"))
                .expectStatus()
                .isEqualTo(HttpStatus.UNPROCESSABLE_CONTENT)
                .expectBody()
                .jsonPath("$.type")
                .isEqualTo("/problems/insufficient-funds")
                .jsonPath("$.status")
                .isEqualTo(422)
                .jsonPath("$.transferStatus")
                .isEqualTo("REJECTED");

        client.get()
                .uri("/transfers?idempotencyKey=" + key)
                .exchange()
                .expectBody()
                .jsonPath("$[0].status")
                .isEqualTo("REJECTED");
    }

    @Test
    @DisplayName("the same key and body after a rejection replays the same 422")
    void replayRejected() {
        var key = "k-" + UUID.randomUUID();
        var transferId = post(key, body(bob, alice, 1, "EUR"))
                .expectStatus()
                .isEqualTo(HttpStatus.UNPROCESSABLE_CONTENT)
                .returnResult(Map.class)
                .getResponseBody()
                .get("transferId");
        fixtures.transfer(alice, bob, 500);

        post(key, body(bob, alice, 1, "EUR"))
                .expectStatus()
                .isEqualTo(HttpStatus.UNPROCESSABLE_CONTENT)
                .expectHeader()
                .valueEquals(TransferController.REPLAYED, "true")
                .expectBody()
                .jsonPath("$.type")
                .isEqualTo("/problems/insufficient-funds")
                .jsonPath("$.transferId")
                .isEqualTo(transferId);
        assertThat(transfersWithKey(key)).isOne();
    }

    @Test
    @DisplayName("the same key with another body is a 422 mismatch and changes nothing")
    void mismatch() {
        var key = "k-" + UUID.randomUUID();
        post(key, body(alice, bob, 400, "EUR")).expectStatus().isCreated();

        post(key, body(alice, bob, 401, "EUR"))
                .expectStatus()
                .isEqualTo(HttpStatus.UNPROCESSABLE_CONTENT)
                .expectBody()
                .jsonPath("$.type")
                .isEqualTo("/problems/idempotency-key-mismatch");

        assertThat(fixtures.balance(alice)).isEqualTo(600);
        assertThat(transfersWithKey(key)).isOne();
    }

    @Test
    @DisplayName("a currency that differs from the accounts is a 422 and is not stored")
    void currencyMismatch() {
        var key = "k-" + UUID.randomUUID();
        post(key, body(alice, bob, 1, "USD"))
                .expectStatus()
                .isEqualTo(HttpStatus.UNPROCESSABLE_CONTENT)
                .expectBody()
                .jsonPath("$.type")
                .isEqualTo("/problems/currency-mismatch");

        assertThat(transfersWithKey(key)).isZero();
    }

    @Test
    @DisplayName("an unknown account is a 404 and is not stored")
    void unknownAccount() {
        var key = "k-" + UUID.randomUUID();
        post(key, body(alice, new AccountId(UUID.randomUUID()), 1, "EUR"))
                .expectStatus()
                .isNotFound()
                .expectBody()
                .jsonPath("$.type")
                .isEqualTo("/problems/account-not-found");

        assertThat(transfersWithKey(key)).isZero();
        post(key, body(alice, bob, 1, "EUR")).expectStatus().isCreated();
    }

    @Test
    @DisplayName("a missing idempotency key is a 400")
    void missingKey() {
        post(null, body(alice, bob, 1, "EUR"))
                .expectStatus()
                .isBadRequest()
                .expectBody()
                .jsonPath("$.type")
                .isEqualTo("/problems/idempotency-key-required");
    }

    @Test
    @DisplayName("a malformed idempotency key is a 400")
    void malformedKey() {
        post("has space", body(alice, bob, 1, "EUR"))
                .expectStatus()
                .isBadRequest()
                .expectBody()
                .jsonPath("$.type")
                .isEqualTo("/problems/idempotency-key-required");
    }

    @Test
    @DisplayName("a negative amount is a 400")
    void negativeAmount() {
        post("k-" + UUID.randomUUID(), body(alice, bob, -5, "EUR"))
                .expectStatus()
                .isBadRequest();
    }

    @Test
    @DisplayName("a transfer to the same account is a 400")
    void sameAccount() {
        post("k-" + UUID.randomUUID(), body(alice, alice, 5, "EUR"))
                .expectStatus()
                .isBadRequest()
                .expectBody()
                .jsonPath("$.type")
                .isEqualTo("/problems/invalid-request");
    }

    @Test
    @DisplayName("a transfer can be read back by id, and an unknown id is a 404")
    void getById() {
        var id = post("k-" + UUID.randomUUID(), body(alice, bob, 400, "EUR"))
                .returnResult(TransferResponse.class)
                .getResponseBody()
                .id();

        client.get().uri("/transfers/" + id).exchange().expectStatus().isOk();
        client.get()
                .uri("/transfers/" + UUID.randomUUID())
                .exchange()
                .expectStatus()
                .isNotFound();
    }

    @Test
    @DisplayName("a missing X-Request-Id gets a generated one in the response")
    void generatedCorrelationId() {
        client.post()
                .uri("/transfers")
                .header("Idempotency-Key", "k-" + UUID.randomUUID())
                .body(body(alice, bob, 1, "EUR"))
                .exchange()
                .expectStatus()
                .isCreated()
                .expectHeader()
                .value(
                        "X-Request-Id",
                        value -> assertThat(value).matches("\\p{XDigit}{8}(-\\p{XDigit}{4}){3}-\\p{XDigit}{12}"));
    }
}
