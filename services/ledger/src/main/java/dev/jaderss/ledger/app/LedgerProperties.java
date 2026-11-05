package dev.jaderss.ledger.app;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

@Validated
@ConfigurationProperties("ledger")
public record LedgerProperties(
        @NotNull Mode mode,
        @NotNull Locking locking,
        @NotBlank String topic) {

    public enum Mode {
        CORRECT,
        NO_IDEMPOTENCY,
        DUAL_WRITE,
        UNORDERED_LOCKS,
        READ_MODIFY_WRITE
    }

    public enum Locking {
        ORDERED
    }
}
