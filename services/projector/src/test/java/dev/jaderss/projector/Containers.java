package dev.jaderss.projector;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.context.annotation.Bean;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.postgresql.PostgreSQLContainer;

@TestConfiguration(proxyBeanMethods = false)
class Containers {

    @Bean
    @ServiceConnection
    PostgreSQLContainer postgres() {
        return new PostgreSQLContainer("postgres:18.6");
    }

    @Bean
    @ServiceConnection
    KafkaContainer kafka() {
        return new KafkaContainer("apache/kafka:4.4.0");
    }

    @Bean
    NewTopic transfers() {
        return new NewTopic("ledger.transfers", 3, (short) 1);
    }

    @Bean
    NewTopic deadLetters() {
        return new NewTopic("ledger.transfers.DLT", 1, (short) 1);
    }
}
