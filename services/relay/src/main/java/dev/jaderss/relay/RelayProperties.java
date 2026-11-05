package dev.jaderss.relay;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;
import java.time.Duration;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

@Validated
@ConfigurationProperties("relay")
public record RelayProperties(
        @NotBlank String topic,
        @Positive int batch,
        @NotNull Duration interval,
        @NotNull Kafka kafka,
        @NotNull Duration idleInTransactionTimeout) {

    public record Kafka(
            @NotNull Duration maxBlock,
            @NotNull Duration requestTimeout,
            @NotNull Duration deliveryTimeout) {}

    public RelayProperties {
        if (kafka != null
                && idleInTransactionTimeout != null
                && kafka.maxBlock().plus(kafka.deliveryTimeout()).compareTo(idleInTransactionTimeout) >= 0) {
            throw new IllegalArgumentException(
                    "max block %s plus delivery timeout %s must stay below the idle in transaction timeout %s"
                            .formatted(kafka.maxBlock(), kafka.deliveryTimeout(), idleInTransactionTimeout));
        }
    }

    public Duration batchDeadline() {
        return kafka.maxBlock().plus(kafka.deliveryTimeout());
    }
}
