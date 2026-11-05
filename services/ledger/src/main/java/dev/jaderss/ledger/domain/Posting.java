package dev.jaderss.ledger.domain;

import java.util.Objects;

public record Posting(AccountId accountId, Money amount) {

    public Posting {
        Objects.requireNonNull(accountId, "accountId");
        Objects.requireNonNull(amount, "amount");
        if (amount.amountMinor() == 0) {
            throw new IllegalArgumentException("posting amount must not be zero");
        }
    }
}
