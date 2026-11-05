package dev.jaderss.ledger.domain;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import dev.jaderss.ledger.domain.TransferDecision.Post;
import dev.jaderss.ledger.domain.TransferDecision.Reject;
import java.util.UUID;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class TransferDecisionTest {

    private static Account account(long balance, boolean overdraft) {
        return new Account(new AccountId(UUID.randomUUID()), Money.of(balance, "EUR"), overdraft);
    }

    @Test
    @DisplayName("posts a debit and a credit that sum to zero and moves both balances")
    void posts() {
        var from = account(1_000, false);
        var to = account(0, false);

        var post = (Post) TransferDecision.decide(from, to, Money.of(400, "EUR"));

        assertThat(post.postings())
                .containsExactly(
                        new Posting(from.id(), Money.of(-400, "EUR")), new Posting(to.id(), Money.of(400, "EUR")));
        assertThat(post.from().balance()).isEqualTo(Money.of(600, "EUR"));
        assertThat(post.to().balance()).isEqualTo(Money.of(400, "EUR"));
    }

    @Test
    @DisplayName("allows a transfer that empties the account exactly")
    void exactBalance() {
        assertThat(TransferDecision.decide(account(400, false), account(0, false), Money.of(400, "EUR")))
                .isInstanceOf(Post.class);
    }

    @Test
    @DisplayName("rejects a transfer the account cannot cover")
    void insufficientFunds() {
        assertThat(TransferDecision.decide(account(399, false), account(0, false), Money.of(400, "EUR")))
                .isEqualTo(new Reject(RejectReason.INSUFFICIENT_FUNDS));
    }

    @Test
    @DisplayName("lets an overdraft account go below zero")
    void overdraft() {
        var post = (Post) TransferDecision.decide(account(0, true), account(0, false), Money.of(400, "EUR"));

        assertThat(post.from().balance()).isEqualTo(Money.of(-400, "EUR"));
    }

    @Test
    @DisplayName("refuses an amount in another currency than the accounts")
    void currencyMismatch() {
        assertThatThrownBy(() -> TransferDecision.decide(account(1_000, false), account(0, false), Money.of(1, "USD")))
                .isInstanceOf(CurrencyMismatchException.class);
    }

    @Test
    @DisplayName("refuses a credit account in another currency")
    void creditCurrencyMismatch() {
        var usd = new Account(new AccountId(UUID.randomUUID()), Money.of(0, "USD"), false);
        assertThatThrownBy(() -> TransferDecision.decide(account(1_000, false), usd, Money.of(1, "EUR")))
                .isInstanceOf(CurrencyMismatchException.class);
    }

    @Test
    @DisplayName("refuses a zero amount")
    void zeroAmount() {
        assertThatThrownBy(() -> TransferDecision.decide(account(1_000, false), account(0, false), Money.of(0, "EUR")))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
