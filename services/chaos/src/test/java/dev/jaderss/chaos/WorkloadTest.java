package dev.jaderss.chaos;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.equalTo;
import static com.github.tomakehurst.wiremock.client.WireMock.post;
import static com.github.tomakehurst.wiremock.client.WireMock.postRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig;
import static com.github.tomakehurst.wiremock.stubbing.Scenario.STARTED;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.github.tomakehurst.wiremock.junit5.WireMockExtension;
import dev.jaderss.chaos.Config.Load;
import dev.jaderss.chaos.JsonLog.Body;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.SplittableRandom;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;

class WorkloadTest {

    @RegisterExtension
    static final WireMockExtension LEDGER = WireMockExtension.newInstance()
            .options(wireMockConfig().dynamicPort())
            .build();

    private static final Body BODY = new Body("a", "b", 1, "EUR");
    private static final String POSTED = "{\"id\":\"t\",\"status\":\"POSTED\"}";

    @TempDir
    Path dir;

    private JsonLog log;

    @BeforeEach
    void openLog() throws IOException {
        log = new JsonLog(dir.resolve("history.jsonl"), Chaos.json());
    }

    @AfterEach
    void closeLog() throws IOException {
        log.close();
    }

    private Workload workload(int accounts, double duplicateRate, double mismatchRate) {
        var load = new Load(accounts, 2, 2, 3, Duration.ofSeconds(1), duplicateRate, mismatchRate, 100, 10);
        return new Workload(new LedgerClient(URI.create(LEDGER.baseUrl()), Chaos.json()), log, load, 7);
    }

    @Test
    @DisplayName("an unknown outcome is retried with the same key until a definite answer")
    void retriesWithTheSameKey() {
        LEDGER.stubFor(post("/transfers")
                .inScenario("flaky")
                .whenScenarioStateIs(STARTED)
                .willReturn(aResponse().withStatus(503))
                .willSetStateTo("still down"));
        LEDGER.stubFor(post("/transfers")
                .inScenario("flaky")
                .whenScenarioStateIs("still down")
                .willReturn(aResponse().withStatus(503))
                .willSetStateTo("up"));
        LEDGER.stubFor(post("/transfers")
                .inScenario("flaky")
                .whenScenarioStateIs("up")
                .willReturn(aResponse().withStatus(201).withBody(POSTED)));
        var workload = workload(2, 0, 0);

        workload.send(0, "k", BODY);

        LEDGER.verify(3, postRequestedFor(urlEqualTo("/transfers")).withHeader("Idempotency-Key", equalTo("k")));
        assertThat(workload.settledCount()).isOne();
    }

    @Test
    @DisplayName("a key gives up after the retry budget and is not offered for mismatches")
    void givesUpAfterTheBudget() {
        LEDGER.stubFor(post("/transfers").willReturn(aResponse().withStatus(503)));
        var workload = workload(2, 0, 0);

        workload.send(0, "k", BODY);

        LEDGER.verify(4, postRequestedFor(urlEqualTo("/transfers")));
        assertThat(workload.settledCount()).isZero();
    }

    @Test
    @DisplayName("a refused transfer is not reused for mismatches")
    void refusedIsNotReused() {
        LEDGER.stubFor(post("/transfers").willReturn(aResponse().withStatus(422)));
        var workload = workload(2, 0, 0);

        workload.send(0, "k", BODY);

        assertThat(workload.settledCount()).isZero();
    }

    @Test
    @DisplayName("setup, funding and a short run send transfers with duplicates and mismatches")
    void setupFundAndRun() throws Exception {
        LEDGER.stubFor(post("/accounts").willReturn(aResponse().withStatus(201)));
        LEDGER.stubFor(post("/transfers").willReturn(aResponse().withStatus(201).withBody(POSTED)));
        var workload = workload(4, 0.3, 0.3);

        workload.setup();
        workload.fund();
        workload.run(Instant.now().plusMillis(300));
        log.close();

        assertThat(workload.accounts()).hasSize(4);
        LEDGER.verify(5, postRequestedFor(urlEqualTo("/accounts")));
        assertThat(LEDGER.findAll(postRequestedFor(urlEqualTo("/transfers")))).hasSizeGreaterThan(4);
        assertThat(Files.readString(dir.resolve("history.jsonl")))
                .contains("\"worker\":-1", "\"mismatch\":true", "\"phase\":\"ok\"");
    }

    @Test
    @DisplayName("funding stops on a refusal")
    void fundingStopsOnRefusal() throws Exception {
        LEDGER.stubFor(post("/accounts").willReturn(aResponse().withStatus(201)));
        LEDGER.stubFor(post("/transfers").willReturn(aResponse().withStatus(404)));
        var workload = workload(2, 0, 0);
        workload.setup();

        assertThatThrownBy(workload::fund).isInstanceOf(IOException.class).hasMessageContaining("refused: 404");
    }

    @Test
    @DisplayName("a random body never picks the same account twice and stays in the amount range")
    void randomBody() throws Exception {
        LEDGER.stubFor(post("/accounts").willReturn(aResponse().withStatus(201)));
        var workload = workload(3, 0, 0);
        workload.setup();
        var random = new SplittableRandom(1);

        for (var i = 0; i < 1_000; i++) {
            var body = workload.randomBody(random);
            assertThat(body.from()).isNotEqualTo(body.to());
            assertThat(body.amount()).isBetween(1L, 10L);
        }
    }
}
