package dev.jaderss.projector;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.stream.IntStream;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.jdbi.v3.core.Jdbi;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.kafka.core.KafkaTemplate;
import org.testcontainers.kafka.KafkaContainer;
import tools.jackson.databind.json.JsonMapper;

@SpringBootTest(properties = "spring.datasource.password=unused")
@Import(Containers.class)
class ProjectorTest {

    @Autowired
    KafkaTemplate<String, String> kafka;

    @Autowired
    KafkaContainer container;

    @Autowired
    Jdbi jdbi;

    @Autowired
    JsonMapper json;

    @Autowired
    BalanceProjection projection;

    private static TransferPosted event(UUID from, UUID to, long amount) {
        return new TransferPosted(
                UUID.randomUUID(),
                "TransferPosted",
                UUID.randomUUID(),
                "req",
                Instant.now(),
                "EUR",
                List.of(new TransferPosted.Line(from, -amount), new TransferPosted.Line(to, amount)));
    }

    private void publish(TransferPosted event) {
        publishRaw(event.transferId().toString(), json.writeValueAsString(event));
    }

    private void publishRaw(String key, String value) {
        kafka.send("ledger.transfers", key, value).join();
    }

    private long balance(UUID account) {
        return jdbi.withHandle(
                h -> h.createQuery("select coalesce(sum(balance_minor), 0) from balance where account_id = :id")
                        .bind("id", account)
                        .mapTo(Long.class)
                        .one());
    }

    private long processed(UUID eventId) {
        return jdbi.withHandle(h -> h.createQuery("select count(*) from processed_event where event_id = :id")
                .bind("id", eventId)
                .mapTo(Long.class)
                .one());
    }

    private void awaitBalance(UUID account, long expected) {
        await().atMost(Duration.ofSeconds(30))
                .untilAsserted(() -> assertThat(balance(account)).isEqualTo(expected));
    }

    private List<String> deadLetterKeys() {
        try (var consumer = new KafkaConsumer<String, String>(Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
                container.getBootstrapServers(),
                ConsumerConfig.GROUP_ID_CONFIG,
                "test-" + UUID.randomUUID(),
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,
                "earliest",
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class))) {
            consumer.subscribe(List.of("ledger.transfers.DLT"));
            var keys = new ArrayList<String>();
            consumer.poll(Duration.ofSeconds(2)).forEach(r -> keys.add(r.key()));
            return keys;
        }
    }

    @Test
    @DisplayName("the same event delivered twice is applied once")
    void duplicateApplied() {
        var a = UUID.randomUUID();
        var b = UUID.randomUUID();
        var event = event(a, b, 250);

        publish(event);
        publish(event);
        var marker = event(a, b, 1);
        publish(marker);

        awaitBalance(b, 251);
        assertThat(balance(a)).isEqualTo(-251);
        assertThat(processed(event.eventId())).isOne();
    }

    @Test
    @DisplayName("a replay after a crash between commit and ack changes nothing")
    void replayAfterCommit() {
        var a = UUID.randomUUID();
        var event = event(a, UUID.randomUUID(), 40);

        assertThat(projection.apply(event)).isTrue();
        assertThat(projection.apply(event)).isFalse();
        assertThat(balance(a)).isEqualTo(-40);
    }

    @Test
    @DisplayName("events on overlapping accounts across partitions, each sent twice, end equal to the sum of postings")
    void parallelPartitions() {
        var accounts = IntStream.range(0, 4).mapToObj(_ -> UUID.randomUUID()).toList();
        var expected = new HashMap<UUID, Long>();
        var random = new Random(42);

        for (var i = 0; i < 300; i++) {
            var from = accounts.get(random.nextInt(accounts.size()));
            var to = accounts.get(random.nextInt(accounts.size()));
            if (from.equals(to)) {
                continue;
            }
            var amount = 1 + random.nextInt(500);
            var event = event(from, to, amount);
            publish(event);
            publish(event);
            expected.merge(from, (long) -amount, Long::sum);
            expected.merge(to, (long) amount, Long::sum);
        }

        await().atMost(Duration.ofSeconds(60))
                .untilAsserted(() -> accounts.forEach(account -> assertThat(balance(account))
                        .as(account.toString())
                        .isEqualTo(expected.getOrDefault(account, 0L))));
    }

    @Test
    @DisplayName("an unparseable record goes to the DLT and later events are still applied")
    void unparseableToDeadLetter() {
        var key = "broken-" + UUID.randomUUID();
        publishRaw(key, "{not json");
        var a = UUID.randomUUID();
        publish(event(a, UUID.randomUUID(), 10));

        awaitBalance(a, -10);
        await().atMost(Duration.ofSeconds(20)).until(() -> deadLetterKeys().contains(key));
    }

    @Test
    @DisplayName("an event with a malformed currency goes to the DLT instead of retrying forever")
    void badCurrencyToDeadLetter() {
        var a = UUID.randomUUID();
        var transferId = UUID.randomUUID().toString();
        publishRaw(transferId, """
                {"eventId":"%s","type":"TransferPosted","transferId":"%s","currency":"EURO",
                 "postings":[{"accountId":"%s","amountMinor":-10},{"accountId":"%s","amountMinor":10}]}
                """.formatted(UUID.randomUUID(), transferId, a, UUID.randomUUID()));

        await().atMost(Duration.ofSeconds(20)).until(() -> deadLetterKeys().contains(transferId));
        assertThat(balance(a)).isZero();
    }

    @Test
    @DisplayName("an event with an empty account id goes to the DLT instead of retrying forever")
    void emptyAccountToDeadLetter() {
        var transferId = UUID.randomUUID().toString();
        publishRaw(transferId, """
                {"eventId":"%s","type":"TransferPosted","transferId":"%s","currency":"EUR",
                 "postings":[{"accountId":"","amountMinor":-5},{"accountId":"","amountMinor":5}]}
                """.formatted(UUID.randomUUID(), transferId));

        await().atMost(Duration.ofSeconds(20)).until(() -> deadLetterKeys().contains(transferId));
    }

    @Test
    @DisplayName("an event whose postings do not sum to zero goes to the DLT and is not applied")
    void unbalancedToDeadLetter() {
        var a = UUID.randomUUID();
        var transferId = UUID.randomUUID().toString();
        publishRaw(transferId, """
                {"eventId":"%s","type":"TransferPosted","transferId":"%s","currency":"EUR",
                 "postings":[{"accountId":"%s","amountMinor":-10}]}
                """.formatted(UUID.randomUUID(), transferId, a));

        await().atMost(Duration.ofSeconds(20)).until(() -> deadLetterKeys().contains(transferId));
        assertThat(balance(a)).isZero();
    }
}
