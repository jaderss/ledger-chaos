package dev.jaderss.ledger.app;

import jakarta.validation.constraints.NotNull;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

@Validated
@ConfigurationProperties("ledger")
public record LedgerProperties(@NotNull Mode mode, @NotNull Locking locking) {

    public enum Mode {
        CORRECT
    }

    public enum Locking {
        ORDERED
    }
}
