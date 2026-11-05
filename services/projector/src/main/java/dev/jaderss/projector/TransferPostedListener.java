package dev.jaderss.projector;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;
import tools.jackson.databind.json.JsonMapper;

@Component
class TransferPostedListener {

    private static final Logger log = LoggerFactory.getLogger(TransferPostedListener.class);

    private final BalanceProjection projection;
    private final JsonMapper json;

    TransferPostedListener(BalanceProjection projection, JsonMapper json) {
        this.projection = projection;
        this.json = json;
    }

    @KafkaListener(topics = "${projector.topic}", groupId = "${projector.group}")
    void on(ConsumerRecord<String, String> message, Acknowledgment ack) {
        var event = json.readValue(message.value(), TransferPosted.class);
        if (!projection.apply(event)) {
            log.info("event {} for transfer {} already applied", event.eventId(), event.transferId());
        }
        ack.acknowledge();
    }
}
