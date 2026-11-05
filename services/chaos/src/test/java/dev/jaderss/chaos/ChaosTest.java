package dev.jaderss.chaos;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.any;
import static com.github.tomakehurst.wiremock.client.WireMock.anyUrl;
import static com.github.tomakehurst.wiremock.client.WireMock.delete;
import static com.github.tomakehurst.wiremock.client.WireMock.post;
import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig;
import static org.assertj.core.api.Assertions.assertThat;

import com.github.tomakehurst.wiremock.junit5.WireMockExtension;
import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;

class ChaosTest {

    @RegisterExtension
    static final WireMockExtension LEDGER = WireMockExtension.newInstance()
            .options(wireMockConfig().dynamicPort())
            .build();

    @RegisterExtension
    static final WireMockExtension TOXIPROXY = WireMockExtension.newInstance()
            .options(wireMockConfig().dynamicPort())
            .build();

    @TempDir
    Path dir;

    @Test
    @DisplayName("a run against fakes writes the history, the faults and run.json, and kills reach Docker")
    void runsAgainstFakes() throws Exception {
        LEDGER.stubFor(post("/accounts").willReturn(aResponse().withStatus(201)));
        LEDGER.stubFor(post("/transfers")
                .willReturn(aResponse().withStatus(201).withBody("{\"id\":\"t\",\"status\":\"POSTED\"}")));
        TOXIPROXY.stubFor(delete(anyUrl()).willReturn(aResponse().withStatus(204)));
        TOXIPROXY.stubFor(any(anyUrl()).atPriority(10).willReturn(aResponse().withStatus(200)));
        try (var docker = new FakeDocker()) {
            var config = Config.from(Map.ofEntries(
                    Map.entry("LEDGER_URL", LEDGER.baseUrl()),
                    Map.entry("TOXIPROXY_URL", TOXIPROXY.baseUrl()),
                    Map.entry("DOCKER_SOCKET", docker.socket().toString()),
                    Map.entry("RUN_DIR", dir.toString()),
                    Map.entry("SCENARIO", "kills"),
                    Map.entry("ACCOUNTS", "3"),
                    Map.entry("WORKERS", "2"),
                    Map.entry("DURATION", "300ms"),
                    Map.entry("FAULT_INTERVAL", "50ms"),
                    Map.entry("FAULT_DURATION", "10ms"),
                    Map.entry("SEED", "7")));
            var out = new ByteArrayOutputStream();

            Chaos.run(config, new PrintStream(out, true, StandardCharsets.UTF_8));

            assertThat(Files.readString(dir.resolve("history.jsonl"))).contains("\"phase\":\"ok\"");
            assertThat(Files.readString(dir.resolve("faults.jsonl"))).contains("\"phase\":\"start\"");
            var run = Chaos.json().readTree(dir.resolve("run.json").toFile());
            assertThat(run.get("seed").asLong()).isEqualTo(7);
            assertThat(run.get("scenario").asString()).isEqualTo("kills");
            assertThat(run.get("duration").asString()).isEqualTo("300ms");
            assertThat(String.join(" ", docker.actions())).contains("kill?signal=SIGKILL");
            assertThat(out.toString(StandardCharsets.UTF_8)).contains("seed 7, scenario kills, 2 workers for 300ms");
        }
    }
}
