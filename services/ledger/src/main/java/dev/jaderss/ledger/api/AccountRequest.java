package dev.jaderss.ledger.api;

import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Pattern;
import java.util.UUID;

record AccountRequest(
        @NotNull UUID id,
        @NotNull @Pattern(regexp = "[A-Z]{3}") String currency,
        boolean allowOverdraft) {}
