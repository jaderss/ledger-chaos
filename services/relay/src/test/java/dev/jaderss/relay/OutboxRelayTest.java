package dev.jaderss.relay;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.transaction.support.TransactionTemplate;
import org.testcontainers.kafka.KafkaContainer;

@SpringBootTest(
        properties = {
            "spring.sql.init.mode=always",
            "relay.interval=1h",
            "spring.datasource.password=unused",
            "relay.kafka.max-block=1s",
            "relay.kafka.request-timeout=1s",
            "relay.kafka.delivery-timeout=2s",
            "relay.idle-in-transaction-timeout=5s",
        })
@Import(Containers.class)
class OutboxRelayTest {

    @Autowired
    OutboxRelay relay;

    @Autowired
    JdbcClient jdbc;

    @Autowired
    TransactionTemplate transaction;

    @Autowired
    KafkaContainer kafka;

    KafkaConsumer<String, String> consumer;
    List<ConsumerRecord<String, String>> seen = new ArrayList<>();

    @BeforeEach
    void setUp() {
        jdbc.sql("update outbox set published_at = now() where published_at is null")
                .update();
        consumer = new KafkaConsumer<>(Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
                kafka.getBootstrapServers(),
                ConsumerConfig.GROUP_ID_CONFIG,
                "test-" + UUID.randomUUID(),
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,
                "earliest",
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class));
        consumer.subscribe(List.of("ledger.transfers"));
    }

    @AfterEach
    void tearDown() {
        consumer.close();
    }

    private UUID insertRow() {
        var eventId = UUID.randomUUID();
        var transferId = UUID.randomUUID();
        jdbc.sql("insert into outbox (id, transfer_id, payload) values (:id, :transferId, cast(:payload as jsonb))")
                .param("id", eventId)
                .param("transferId", transferId)
                .param("payload", """
                        {"eventId": "%s", "transferId": "%s", "correlationId": "req-%s"}
                        """.formatted(eventId, transferId, transferId))
                .update();
        return transferId;
    }

    private Map<String, List<ConsumerRecord<String, String>>> recordsFor(List<UUID> transferIds, int expected) {
        var keys = transferIds.stream().map(UUID::toString).toList();
        await().atMost(Duration.ofSeconds(20)).until(() -> {
            consumer.poll(Duration.ofMillis(200)).forEach(seen::add);
            return seen.stream().filter(r -> keys.contains(r.key())).count() >= expected;
        });
        return seen.stream().filter(r -> keys.contains(r.key())).collect(Collectors.groupingBy(ConsumerRecord::key));
    }

    private long unpublished() {
        return jdbc.sql("select count(*) from outbox where published_at is null")
                .query(Long.class)
                .single();
    }

    @Test
    @DisplayName("each row goes out once, keyed by transfer id with the correlation header, and is marked published")
    void publishesAndMarks() {
        var transferId = insertRow();

        assertThat(relay.publishBatch()).isOne();

        var record =
                recordsFor(List.of(transferId), 1).get(transferId.toString()).getFirst();
        assertThat(new String(
                        record.headers()
                                .lastHeader(OutboxRelay.CORRELATION_HEADER)
                                .value(),
                        StandardCharsets.UTF_8))
                .isEqualTo("req-" + transferId);
        assertThat(record.value()).contains(transferId.toString());
        assertThat(unpublished()).isZero();
    }

    @Test
    @DisplayName(
            "a crash between send and mark leaves the row unpublished, and the next batch sends the same event again")
    void crashBetweenSendAndMark() {
        var transferId = insertRow();

        transaction.executeWithoutResult(status -> {
            relay.publishBatch();
            status.setRollbackOnly();
        });
        assertThat(unpublished()).isOne();
        relay.publishBatch();

        var copies = recordsFor(List.of(transferId), 2).get(transferId.toString());
        assertThat(copies)
                .hasSize(2)
                .extracting(ConsumerRecord::value)
                .containsOnly(copies.getFirst().value());
        assertThat(unpublished()).isZero();
    }

    @Test
    @DisplayName("with Kafka unavailable the batch rolls back inside the deadline and goes out once Kafka is back")
    void kafkaUnavailable() {
        var transferId = insertRow();
        var docker = kafka.getDockerClient();

        docker.pauseContainerCmd(kafka.getContainerId()).exec();
        try {
            assertThatThrownBy(relay::publishBatch).isInstanceOf(IllegalStateException.class);
            assertThat(unpublished()).isOne();
        } finally {
            docker.unpauseContainerCmd(kafka.getContainerId()).exec();
        }

        await().atMost(Duration.ofSeconds(30)).ignoreExceptions().until(() -> relay.publishBatch() == 1);
        assertThat(recordsFor(List.of(transferId), 1)).containsKey(transferId.toString());
        assertThat(unpublished()).isZero();
    }

    @Test
    @DisplayName("parallel batches never claim the same row")
    void skipLocked() throws Exception {
        var transferIds = IntStream.range(0, 200).mapToObj(_ -> insertRow()).toList();

        try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
            List<Callable<Integer>> drains = IntStream.range(0, 8)
                    .<Callable<Integer>>mapToObj(_ -> () -> {
                        var total = 0;
                        int sent;
                        while ((sent = relay.publishBatch()) > 0) {
                            total += sent;
                        }
                        return total;
                    })
                    .toList();
            var total = 0;
            for (var future : executor.invokeAll(drains)) {
                total += future.get();
            }
            assertThat(total).isEqualTo(200);
        }

        var byKey = recordsFor(transferIds, 200);
        assertThat(byKey)
                .hasSize(200)
                .allSatisfy((key, records) -> assertThat(records).hasSize(1));
        assertThat(unpublished()).isZero();
    }
}
