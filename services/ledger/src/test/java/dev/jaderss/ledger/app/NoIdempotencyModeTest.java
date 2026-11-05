package dev.jaderss.ledger.app;

import static org.assertj.core.api.Assertions.assertThat;

import dev.jaderss.ledger.Fixtures;
import dev.jaderss.ledger.PostgresContainer;
import dev.jaderss.ledger.app.TransferOutcome.Created;
import dev.jaderss.ledger.domain.Money;
import dev.jaderss.ledger.domain.Transfer;
import dev.jaderss.ledger.domain.TransferCommand;
import dev.jaderss.ledger.domain.TransferStatus;
import dev.jaderss.ledger.persistence.LedgerDatabase;
import java.util.List;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;

@SpringBootTest(properties = "ledger.mode=no-idempotency")
@Import({PostgresContainer.class, Fixtures.class})
class NoIdempotencyModeTest {

    @Autowired
    TransferService service;

    @Autowired
    LedgerDatabase database;

    @Autowired
    Fixtures fixtures;

    @Test
    @DisplayName("a retry with the same key posts the transfer again")
    void retryPostsAgain() {
        var from = fixtures.account(1_000);
        var to = fixtures.account(0);
        var command = Fixtures.command(from, to, 100);

        var first = service.transfer(command);
        var retry = service.transfer(command);

        assertThat(first).isInstanceOf(Created.class).isNotEqualTo(retry);
        assertThat(retry).isInstanceOf(Created.class);
        List<Transfer> posted = database.withStores(stores -> stores.transfers().findByKey(command.key()));
        assertThat(posted).hasSize(2).allMatch(t -> t.status() == TransferStatus.POSTED);
        assertThat(fixtures.balance(from)).isEqualTo(800);
    }

    @Test
    @DisplayName("the same key with another body posts that body instead of answering a mismatch")
    void mismatchPosts() {
        var from = fixtures.account(1_000);
        var to = fixtures.account(0);
        var command = Fixtures.command(from, to, 100);
        service.transfer(command);

        var mismatch = service.transfer(
                new TransferCommand(command.key(), command.correlationId(), from, to, Money.of(101, "EUR")));

        assertThat(mismatch).isInstanceOf(Created.class);
        assertThat(fixtures.balance(to)).isEqualTo(201);
    }
}
