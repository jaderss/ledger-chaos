package dev.jaderss.ledger.domain;

import static org.assertj.core.api.Assertions.assertThat;

import dev.jaderss.ledger.domain.TransferDecision.Post;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.stream.IntStream;
import net.jqwik.api.Arbitraries;
import net.jqwik.api.Arbitrary;
import net.jqwik.api.Combinators;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.Provide;
import net.jqwik.api.constraints.IntRange;

class LedgerPropertyTest {

    private static final int ACCOUNTS = 5;

    record Move(int from, int to, long amount) {}

    @Property
    void randomTransfersKeepTheLedgerBalanced(
            @ForAll("moves") List<Move> moves, @ForAll @IntRange(min = 0, max = ACCOUNTS - 1) int overdraftIndex) {
        var accounts = new ArrayList<Account>();
        IntStream.range(0, ACCOUNTS)
                .forEach(i -> accounts.add(
                        new Account(new AccountId(UUID.randomUUID()), Money.of(0, "EUR"), i == overdraftIndex)));
        var postings = new ArrayList<Posting>();

        for (var move : moves) {
            if (TransferDecision.decide(
                            accounts.get(move.from()), accounts.get(move.to()), Money.of(move.amount(), "EUR"))
                    instanceof Post post) {
                postings.addAll(post.postings());
                accounts.set(move.from(), post.from());
                accounts.set(move.to(), post.to());
            }
        }

        assertThat(sum(postings)).as("I1 postings sum to zero").isZero();
        for (var account : accounts) {
            var own = postings.stream()
                    .filter(p -> p.accountId().equals(account.id()))
                    .toList();
            assertThat(account.balance().amountMinor())
                    .as("I2 balance equals its postings")
                    .isEqualTo(sum(own));
            if (!account.allowOverdraft()) {
                assertThat(sum(own)).as("I3 no overspend").isNotNegative();
            }
        }
    }

    @Provide
    Arbitrary<List<Move>> moves() {
        var index = Arbitraries.integers().between(0, ACCOUNTS - 1);
        return Combinators.combine(index, index, Arbitraries.longs().between(1, 10_000))
                .filter((from, to, amount) -> !from.equals(to))
                .as(Move::new)
                .list()
                .ofMaxSize(200);
    }

    private static long sum(List<Posting> postings) {
        return postings.stream().mapToLong(p -> p.amount().amountMinor()).reduce(0, Math::addExact);
    }
}
