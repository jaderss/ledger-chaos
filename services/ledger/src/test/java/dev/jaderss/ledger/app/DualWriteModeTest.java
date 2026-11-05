package dev.jaderss.ledger.app;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import dev.jaderss.ledger.Fixtures;
import dev.jaderss.ledger.PostgresContainer;
import dev.jaderss.ledger.app.TransferOutcome.Created;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
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
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;
import org.testcontainers.kafka.KafkaContainer;

@SpringBootTest(properties = "ledger.mode=dual-write")
@Import({PostgresContainer.class, DualWriteModeTest.Kafka.class, Fixtures.class})
class DualWriteModeTest {

    @TestConfiguration(proxyBeanMethods = false)
    static class Kafka {

        @Bean
        @ServiceConnection
        KafkaContainer kafka() {
            return new KafkaContainer("apache/kafka:4.4.0");
        }
    }

    @Autowired
    Fixtures fixtures;

    @Autowired
    JdbcClient jdbc;

    @Autowired
    PlatformTransactionManager transactionManager;

    @Autowired
    KafkaContainer kafka;

    KafkaConsumer<String, String> consumer;
    List<ConsumerRecord<String, String>> seen = new ArrayList<>();

    @BeforeEach
    void setUp() {
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

    private List<ConsumerRecord<String, String>> recordsFor(String transferId, Duration wait) {
        var deadline = System.nanoTime() + wait.toNanos();
        while (System.nanoTime() < deadline && seen.stream().noneMatch(r -> transferId.equals(r.key()))) {
            consumer.poll(Duration.ofMillis(200)).forEach(seen::add);
        }
        return seen.stream().filter(r -> transferId.equals(r.key())).toList();
    }

    private long outboxRows(String transferId) {
        return jdbc.sql("select count(*) from outbox where transfer_id = cast(:id as uuid)")
                .param("id", transferId)
                .query(Long.class)
                .single();
    }

    @Test
    @DisplayName("a posted transfer goes straight to the topic after the commit, with no outbox row")
    void publishesAfterCommit() {
        var outcome = fixtures.transfer(fixtures.account(1_000), fixtures.account(0), 100);
        var transferId = ((Created) outcome).transfer().id().value().toString();

        var records = recordsFor(transferId, Duration.ofSeconds(20));

        assertThat(records).singleElement().satisfies(record -> {
            assertThat(record.value()).contains("TransferPosted");
            assertThat(new String(record.headers().lastHeader("correlationId").value(), StandardCharsets.UTF_8))
                    .isEqualTo("test");
        });
        assertThat(outboxRows(transferId)).isZero();
    }

    @Test
    @DisplayName("a transfer whose transaction rolls back publishes nothing")
    void rollbackPublishesNothing() {
        var from = fixtures.account(1_000);
        var to = fixtures.account(0);
        var transaction = new TransactionTemplate(transactionManager);

        var outcome = transaction.execute(status -> {
            status.setRollbackOnly();
            return fixtures.transfer(from, to, 100);
        });
        var transferId = ((Created) outcome).transfer().id().value().toString();

        await().during(Duration.ofSeconds(2))
                .atMost(Duration.ofSeconds(3))
                .until(() -> recordsFor(transferId, Duration.ofMillis(200)).isEmpty());
        assertThat(fixtures.balance(from)).isEqualTo(1_000);
    }
}
