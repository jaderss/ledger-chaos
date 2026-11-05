package dev.jaderss.ledger.app;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import dev.jaderss.ledger.Fixtures;
import dev.jaderss.ledger.PostgresContainer;
import dev.jaderss.ledger.app.TransferOutcome.Created;
import dev.jaderss.ledger.domain.TransferStatus;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.IntStream;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;

@SpringBootTest(
        properties = {
            "ledger.locking=serializable",
            "ledger.serializable-retries=200",
            "spring.datasource.hikari.connection-timeout=60000"
        })
@Import({PostgresContainer.class, Fixtures.class})
class SerializableLockingTest {

    @Autowired
    TransferService service;

    @Autowired
    Fixtures fixtures;

    private static List<TransferOutcome> runAll(List<Callable<TransferOutcome>> tasks) throws Exception {
        try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
            var results = new ArrayList<TransferOutcome>();
            for (Future<TransferOutcome> future : executor.invokeAll(tasks)) {
                results.add(future.get());
            }
            return results;
        }
    }

    private long rollbacks() {
        return fixtures.sql(
                h -> h.createQuery("select xact_rollback from pg_stat_database where datname = current_database()")
                        .mapTo(Long.class)
                        .one());
    }

    @Test
    @DisplayName("transfers in both directions without row locks keep the money, retrying serialization failures")
    void contention() throws Exception {
        var a = fixtures.account(5_000);
        var b = fixtures.account(5_000);
        var rollbacksBefore = rollbacks();

        var outcomes = runAll(IntStream.range(0, 200)
                .<Callable<TransferOutcome>>mapToObj(
                        i -> () -> i % 2 == 0 ? fixtures.transfer(a, b, 1 + i % 7) : fixtures.transfer(b, a, 1 + i % 5))
                .toList());

        assertThat(outcomes).allMatch(Created.class::isInstance);
        assertThat(fixtures.balance(a) + fixtures.balance(b)).isEqualTo(10_000);
        assertThat(fixtures.balance(a)).isEqualTo(fixtures.sumOfPostings(a));
        assertThat(fixtures.balance(b)).isEqualTo(fixtures.sumOfPostings(b));
        await().atMost(Duration.ofSeconds(15)).until(this::rollbacks, rollbacks -> rollbacks > rollbacksBefore);
    }

    @Test
    @DisplayName("an account under parallel debits never goes below zero")
    void noOverspend() throws Exception {
        var poor = fixtures.account(100);
        var sink = fixtures.account(0);

        var outcomes = runAll(IntStream.range(0, 50)
                .<Callable<TransferOutcome>>mapToObj(i -> () -> fixtures.transfer(poor, sink, 10))
                .toList());

        assertThat(outcomes)
                .filteredOn(o -> o instanceof Created(var t) && t.status() == TransferStatus.POSTED)
                .hasSize(10);
        assertThat(fixtures.sumOfPostings(poor)).isZero();
    }

    @Test
    @DisplayName("parallel requests with one key post once")
    void parallelDuplicates() throws Exception {
        var a = fixtures.account(1_000);
        var b = fixtures.account(0);
        var command = Fixtures.command(a, b, 300);

        var outcomes = runAll(IntStream.range(0, 16)
                .<Callable<TransferOutcome>>mapToObj(i -> () -> service.transfer(command))
                .toList());

        assertThat(outcomes).filteredOn(Created.class::isInstance).hasSize(1);
        assertThat(fixtures.balance(a)).isEqualTo(700);
    }
}
