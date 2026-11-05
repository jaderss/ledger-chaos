package dev.jaderss.ledger.domain;

import java.util.Objects;

public record Account(AccountId id, Money balance, boolean allowOverdraft) {

    public Account {
        Objects.requireNonNull(id, "id");
        Objects.requireNonNull(balance, "balance");
    }

    public boolean canCover(Money amount) {
        return allowOverdraft || !balance.minus(amount).isNegative();
    }

    public Account apply(Posting posting) {
        if (!posting.accountId().equals(id)) {
            throw new IllegalArgumentException("posting for " + posting.accountId() + " applied to " + id);
        }
        return new Account(id, balance.plus(posting.amount()), allowOverdraft);
    }
}
