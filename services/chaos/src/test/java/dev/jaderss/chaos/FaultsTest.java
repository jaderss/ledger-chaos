package dev.jaderss.chaos;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.any;
import static com.github.tomakehurst.wiremock.client.WireMock.anyUrl;
import static com.github.tomakehurst.wiremock.client.WireMock.delete;
import static com.github.tomakehurst.wiremock.client.WireMock.equalToJson;
import static com.github.tomakehurst.wiremock.client.WireMock.post;
import static com.github.tomakehurst.wiremock.client.WireMock.postRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import static com.github.tomakehurst.wiremock.client.WireMock.urlPathMatching;
import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.github.tomakehurst.wiremock.junit5.WireMockExtension;
import dev.jaderss.chaos.Faults.Fault;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;

class FaultsTest {

    @RegisterExtension
    static final WireMockExtension TOXIPROXY = WireMockExtension.newInstance()
            .options(wireMockConfig().dynamicPort())
            .build();

    @TempDir
    Path dir;

    private Toxiproxy toxiproxy() {
        return new Toxiproxy(URI.create(TOXIPROXY.baseUrl()), Chaos.json());
    }

    @Test
    @DisplayName("Docker actions reach the compose container over the Unix socket")
    void dockerActions() throws Exception {
        try (var fake = new FakeDocker()) {
            var docker = new Docker(fake.address(), "ledger-chaos", Chaos.json());

            docker.kill("ledger");
            docker.start("ledger");
            docker.stop("kafka");
            docker.restart("postgres");
            docker.unpause("postgres");

            assertThat(fake.actions())
                    .containsExactly(
                            "/containers/c1/kill?signal=SIGKILL",
                            "/containers/c1/start",
                            "/containers/c1/stop?t=0",
                            "/containers/c1/restart?t=0",
                            "/containers/c1/unpause");
            assertThatThrownBy(() -> docker.pause("postgres")).hasMessageContaining("409");
            assertThatThrownBy(() -> docker.kill("missing")).hasMessageContaining("no container for service missing");
        }
    }

    @Test
    @DisplayName("Toxiproxy calls send the proxy state and toxics, and a missing toxic counts as healed")
    void toxiproxyCalls() {
        TOXIPROXY.stubFor(post(urlPathMatching("/proxies/postgres.*"))
                .willReturn(aResponse().withStatus(200)));
        TOXIPROXY.stubFor(delete(anyUrl()).willReturn(aResponse().withStatus(404)));
        TOXIPROXY.stubFor(post("/proxies/broken").willReturn(aResponse().withStatus(400)));
        var toxiproxy = toxiproxy();

        assertThatCode(() -> {
                    toxiproxy.setEnabled("postgres", false);
                    toxiproxy.addToxic("postgres", "latency", "latency", Map.of("latency", 1500L));
                    toxiproxy.removeToxic("postgres", "latency");
                })
                .doesNotThrowAnyException();
        assertThatThrownBy(() -> toxiproxy.setEnabled("broken", true)).hasMessageContaining("400");
        TOXIPROXY.verify(
                postRequestedFor(urlEqualTo("/proxies/postgres")).withRequestBody(equalToJson("{\"enabled\":false}")));
        TOXIPROXY.verify(
                postRequestedFor(urlEqualTo("/proxies/postgres/toxics"))
                        .withRequestBody(
                                equalToJson(
                                        "{\"name\":\"latency\",\"type\":\"latency\",\"stream\":\"downstream\",\"attributes\":{\"latency\":1500}}")));
    }

    @Test
    @DisplayName("the nemesis applies and heals each fault and logs a start and an end for each")
    void nemesisAppliesAndHeals() throws Exception {
        var calls = new CopyOnWriteArrayList<String>();
        var catalog = Map.of("f", new Fault(() -> calls.add("apply"), () -> calls.add("heal")));
        var path = dir.resolve("faults.jsonl");
        try (var log = new JsonLog(path, Chaos.json())) {
            new Nemesis(catalog, List.of("f"), Duration.ofMillis(20), Duration.ofMillis(10), 1, log)
                    .run(Instant.now().plusMillis(90));
        }

        assertThat(calls).hasSizeGreaterThanOrEqualTo(2).startsWith("apply").endsWith("heal");
        assertThat(calls.size() % 2).isZero();
        var lines = Files.readAllLines(path);
        assertThat(lines.stream().filter(l -> l.contains("\"phase\":\"start\"")))
                .hasSameSizeAs(lines.stream()
                        .filter(l -> l.contains("\"phase\":\"end\""))
                        .toList());
    }

    @Test
    @DisplayName("a failing apply is logged as an error and the fault still heals")
    void failingApplyIsLogged() throws Exception {
        var healed = new CopyOnWriteArrayList<String>();
        var catalog = Map.of(
                "f",
                new Fault(
                        () -> {
                            throw new IOException("boom");
                        },
                        () -> healed.add("heal")));
        var path = dir.resolve("faults.jsonl");
        try (var log = new JsonLog(path, Chaos.json())) {
            new Nemesis(catalog, List.of("f"), Duration.ofMillis(20), Duration.ofMillis(5), 1, log)
                    .run(Instant.now().plusMillis(30));
        }

        assertThat(healed).isNotEmpty();
        assertThat(Files.readString(path)).contains("\"phase\":\"error\"", "\"error\":\"boom\"");
    }

    @Test
    @DisplayName("every scenario uses only faults from the catalog")
    void scenariosUseTheCatalog() {
        var catalog = Faults.catalog(null, null, Duration.ofSeconds(1));

        for (var scenario : Scenario.values()) {
            assertThat(catalog.keySet()).containsAll(scenario.faults());
        }
    }

    @Test
    @DisplayName("heal all calls every heal and reports every failure")
    void healAllJoinsErrors() throws Exception {
        TOXIPROXY.stubFor(any(anyUrl()).willReturn(aResponse().withStatus(500)));
        try (var fake = new FakeDocker()) {
            var catalog = Faults.catalog(
                    new Docker(fake.address(), "ledger-chaos", Chaos.json()), toxiproxy(), Duration.ofSeconds(1));

            assertThatThrownBy(() -> Faults.healAll(catalog)).hasMessageContaining("db-cut", "db-latency", "db-reset");
            assertThat(fake.actions()).contains("/containers/c1/start", "/containers/c1/unpause");
        }
    }
}
