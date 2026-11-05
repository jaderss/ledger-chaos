package dev.jaderss.ledger.domain;

import java.util.List;

public sealed interface TransferDecision {

    record Post(List<Posting> postings, Account from, Account to) implements TransferDecision {
        public Post {
            postings = List.copyOf(postings);
        }
    }

    record Reject(RejectReason reason) implements TransferDecision {}

    static TransferDecision decide(Account from, Account to, Money amount) {
        if (!amount.isPositive()) {
            throw new IllegalArgumentException("amount must be positive");
        }
        requireCurrency(from, amount);
        requireCurrency(to, amount);
        if (!from.canCover(amount)) {
            return new Reject(RejectReason.INSUFFICIENT_FUNDS);
        }
        var debit = new Posting(from.id(), amount.negate());
        var credit = new Posting(to.id(), amount);
        return new Post(List.of(debit, credit), from.apply(debit), to.apply(credit));
    }

    private static void requireCurrency(Account account, Money amount) {
        if (!account.balance().currency().equals(amount.currency())) {
            throw new CurrencyMismatchException(account.balance().currency(), amount.currency());
        }
    }
}
