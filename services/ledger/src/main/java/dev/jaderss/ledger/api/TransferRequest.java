package dev.jaderss.ledger.api;

import dev.jaderss.ledger.domain.AccountId;
import dev.jaderss.ledger.domain.IdempotencyKey;
import dev.jaderss.ledger.domain.Money;
import dev.jaderss.ledger.domain.TransferCommand;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Pattern;
import jakarta.validation.constraints.Positive;
import java.util.UUID;

record TransferRequest(
        @NotNull UUID fromAccount,
        @NotNull UUID toAccount,
        @NotNull @Positive Long amountMinor,
        @NotNull @Pattern(regexp = "[A-Z]{3}") String currency) {

    TransferCommand toCommand(IdempotencyKey key, String correlationId) {
        return new TransferCommand(
                key,
                correlationId,
                new AccountId(fromAccount),
                new AccountId(toAccount),
                Money.of(amountMinor, currency));
    }
}
