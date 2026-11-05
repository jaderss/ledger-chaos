package dev.jaderss.ledger.api;

import dev.jaderss.ledger.domain.Account;
import java.util.UUID;

record AccountResponse(UUID id, String currency, boolean allowOverdraft, long balanceMinor) {

    static AccountResponse from(Account account) {
        return new AccountResponse(
                account.id().value(),
                account.balance().currency().getCurrencyCode(),
                account.allowOverdraft(),
                account.balance().amountMinor());
    }
}
