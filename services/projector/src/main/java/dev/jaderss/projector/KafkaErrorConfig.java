package dev.jaderss.projector;

import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.ExponentialBackOff;
import tools.jackson.core.JacksonException;

@Configuration(proxyBeanMethods = false)
class KafkaErrorConfig {

    private static final Logger log = LoggerFactory.getLogger(KafkaErrorConfig.class);

    @Bean
    DefaultErrorHandler errorHandler(KafkaTemplate<String, String> template, @Value("${projector.dlt}") String dlt) {
        var recoverer = new DeadLetterPublishingRecoverer(template, (record, e) -> new TopicPartition(dlt, 0));
        var backOff = new ExponentialBackOff(100, 2);
        backOff.setMaxInterval(5_000);
        var handler = new DefaultErrorHandler(recoverer, backOff);
        handler.addNotRetryableExceptions(JacksonException.class, IllegalArgumentException.class);
        handler.setRetryListeners((record, e, attempt) -> log.warn(
                "retry {} for transfer {} at {}-{}: {}",
                attempt,
                record.key(),
                record.topic(),
                record.offset(),
                e.getMessage()));
        return handler;
    }
}
