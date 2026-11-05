package dev.jaderss.ledger.persistence;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import dev.jaderss.ledger.Fixtures;
import dev.jaderss.ledger.PostgresContainer;
import dev.jaderss.ledger.app.TransferOutcome.Created;
import dev.jaderss.ledger.domain.AccountId;
import dev.jaderss.ledger.domain.Money;
import dev.jaderss.ledger.domain.Posting;
import dev.jaderss.ledger.domain.TransferId;
import dev.jaderss.ledger.persistence.DatabaseException.IntegrityViolation;
import java.util.List;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;

@SpringBootTest
@Import({PostgresContainer.class, Fixtures.class})
class PostingTriggersTest {

    @Autowired
    Fixtures fixtures;

    @Autowired
    LedgerDatabase database;

    private TransferId postedTransfer(AccountId from, AccountId to) {
        return ((Created) fixtures.transfer(from, to, 10)).transfer().id();
    }

    @Test
    @DisplayName("a transfer whose postings do not sum to zero fails at commit")
    void unbalancedFailsAtCommit() {
        var a = fixtures.account(100);
        var b = fixtures.account(0);
        var transferId = postedTransfer(a, b);
        var before = fixtures.sumOfPostings(b);

        assertThatThrownBy(() -> database.inTransaction(Isolation.READ_COMMITTED, stores -> {
                    stores.postings().insert(transferId, List.of(new Posting(b, Money.of(5, "EUR"))));
                    return null;
                }))
                .isInstanceOf(IntegrityViolation.class)
                .hasMessageContaining("do not sum to zero");
        assertThat(fixtures.sumOfPostings(b)).isEqualTo(before);
    }

    @Test
    @DisplayName("postings cannot be updated")
    void noUpdate() {
        var a = fixtures.account(100);
        postedTransfer(a, fixtures.account(0));

        assertThatThrownBy(() ->
                        fixtures.sql(h -> h.createUpdate("update posting set amount_minor = 1 where account_id = :id")
                                .bind("id", a.value())
                                .execute()))
                .isInstanceOf(IntegrityViolation.class)
                .hasMessageContaining("append only");
    }

    @Test
    @DisplayName("postings cannot be deleted")
    void noDelete() {
        var a = fixtures.account(100);
        postedTransfer(a, fixtures.account(0));

        assertThatThrownBy(() -> fixtures.sql(h -> h.createUpdate("delete from posting where account_id = :id")
                        .bind("id", a.value())
                        .execute()))
                .isInstanceOf(IntegrityViolation.class);
    }
}
