package dev.jaderss.relay;

import java.nio.charset.StandardCharsets;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

@Component
class OutboxRelay {

    static final String CORRELATION_HEADER = "correlationId";
    private static final Logger log = LoggerFactory.getLogger(OutboxRelay.class);

    private final OutboxStore outbox;
    private final KafkaTemplate<String, String> kafka;
    private final TransactionTemplate transaction;
    private final RelayProperties properties;

    OutboxRelay(
            OutboxStore outbox,
            KafkaTemplate<String, String> kafka,
            PlatformTransactionManager transactionManager,
            RelayProperties properties) {
        this.outbox = outbox;
        this.kafka = kafka;
        this.transaction = new TransactionTemplate(transactionManager);
        this.properties = properties;
    }

    @Scheduled(fixedDelayString = "${relay.interval}")
    void drain() {
        try {
            while (publishBatch() == properties.batch()) {
                log.debug("full batch published, polling again");
            }
        } catch (RuntimeException e) {
            log.warn("batch rolled back, rows stay unpublished: {}", e.getMessage());
        }
    }

    int publishBatch() {
        return Objects.requireNonNull(transaction.execute(_ -> {
            var rows = outbox.claim(properties.batch());
            if (rows.isEmpty()) {
                return 0;
            }
            var sends = rows.stream().map(this::send).toArray(CompletableFuture[]::new);
            await(CompletableFuture.allOf(sends));
            outbox.markPublished(rows.stream().map(OutboxStore.Row::id).toList());
            return rows.size();
        }));
    }

    private CompletableFuture<?> send(OutboxStore.Row row) {
        var message = new ProducerRecord<>(properties.topic(), row.transferId().toString(), row.payload());
        if (row.correlationId() != null) {
            message.headers().add(CORRELATION_HEADER, row.correlationId().getBytes(StandardCharsets.UTF_8));
        }
        return kafka.send(message);
    }

    private void await(CompletableFuture<Void> all) {
        try {
            all.get(properties.batchDeadline().toMillis(), TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("interrupted while waiting for acks", e);
        } catch (ExecutionException | TimeoutException e) {
            throw new IllegalStateException("send failed: " + e.getMessage(), e);
        }
    }
}
