package dev.jaderss.ledger.messaging;

import dev.jaderss.ledger.domain.TransferPosted;
import java.nio.charset.StandardCharsets;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;
import tools.jackson.databind.json.JsonMapper;

public class DirectPublisher {

    private static final Logger log = LoggerFactory.getLogger(DirectPublisher.class);
    private static final String CORRELATION_HEADER = "correlationId";

    private final KafkaTemplate<String, String> kafka;
    private final JsonMapper json;
    private final String topic;

    public DirectPublisher(KafkaTemplate<String, String> kafka, JsonMapper json, String topic) {
        this.kafka = kafka;
        this.json = json;
        this.topic = topic;
    }

    public void publishAfterCommit(TransferPosted event) {
        TransactionSynchronizationManager.registerSynchronization(new TransactionSynchronization() {
            @Override
            public void afterCommit() {
                send(event);
            }
        });
    }

    private void send(TransferPosted event) {
        var message = new ProducerRecord<>(topic, event.transferId().toString(), json.writeValueAsString(event));
        message.headers().add(CORRELATION_HEADER, event.correlationId().getBytes(StandardCharsets.UTF_8));
        kafka.send(message).whenComplete((_, failure) -> {
            if (failure != null) {
                log.warn("event for transfer {} lost: {}", event.transferId(), failure.getMessage());
            }
        });
    }
}
