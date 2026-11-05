package dev.jaderss.ledger.app;

import static org.assertj.core.api.Assertions.assertThat;

import dev.jaderss.ledger.Fixtures;
import dev.jaderss.ledger.PostgresContainer;
import dev.jaderss.ledger.app.TransferOutcome.Created;
import dev.jaderss.ledger.app.TransferOutcome.KeyMismatch;
import dev.jaderss.ledger.app.TransferOutcome.Replayed;
import dev.jaderss.ledger.domain.TransferCommand;
import dev.jaderss.ledger.domain.TransferId;
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

@SpringBootTest
@Import({PostgresContainer.class, Fixtures.class})
class TransferConcurrencyTest {

    @Autowired
    TransferService service;

    @Autowired
    Fixtures fixtures;

    private static <T> List<T> runAll(List<Callable<T>> tasks) throws Exception {
        try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
            var results = new ArrayList<T>();
            for (Future<T> future : executor.invokeAll(tasks)) {
                results.add(future.get());
            }
            return results;
        }
    }

    @Test
    @DisplayName("transfers in both directions on the same two accounts keep the money and never deadlock")
    void contention() throws Exception {
        var a = fixtures.account(5_000);
        var b = fixtures.account(5_000);
        var deadlocksBefore = fixtures.deadlocks();

        List<Callable<TransferOutcome>> tasks = IntStream.range(0, 400)
                .<Callable<TransferOutcome>>mapToObj(
                        i -> () -> i % 2 == 0 ? fixtures.transfer(a, b, 1 + i % 7) : fixtures.transfer(b, a, 1 + i % 5))
                .toList();
        var outcomes = runAll(tasks);

        assertThat(outcomes).allMatch(Created.class::isInstance);
        assertThat(fixtures.balance(a) + fixtures.balance(b)).isEqualTo(10_000);
        assertThat(fixtures.balance(a)).isEqualTo(fixtures.sumOfPostings(a));
        assertThat(fixtures.balance(b)).isEqualTo(fixtures.sumOfPostings(b));
        assertThat(fixtures.deadlocks()).isEqualTo(deadlocksBefore);
    }

    @Test
    @DisplayName("an account under parallel debits never goes below zero")
    void noOverspend() throws Exception {
        var poor = fixtures.account(100);
        var sink = fixtures.account(0);

        var outcomes = runAll(IntStream.range(0, 50)
                .<Callable<TransferOutcome>>mapToObj(i -> () -> fixtures.transfer(poor, sink, 10))
                .toList());

        var posted = outcomes.stream()
                .filter(o -> o instanceof Created(var t) && t.status().name().equals("POSTED"))
                .count();
        assertThat(posted).isEqualTo(10);
        assertThat(fixtures.balance(poor)).isZero();
        assertThat(fixtures.sumOfPostings(poor)).isZero();
    }

    @Test
    @DisplayName("parallel requests with one key post once, and every caller sees that transfer")
    void parallelDuplicates() throws Exception {
        var a = fixtures.account(1_000);
        var b = fixtures.account(0);
        TransferCommand command = Fixtures.command(a, b, 300);

        var outcomes = runAll(IntStream.range(0, 32)
                .<Callable<TransferOutcome>>mapToObj(i -> () -> service.transfer(command))
                .toList());

        assertThat(outcomes).filteredOn(Created.class::isInstance).hasSize(1);
        assertThat(outcomes).noneMatch(KeyMismatch.class::isInstance);
        assertThat(outcomes.stream().map(TransferConcurrencyTest::transferId).distinct())
                .hasSize(1);
        assertThat(fixtures.balance(a)).isEqualTo(700);
    }

    private static TransferId transferId(TransferOutcome outcome) {
        return switch (outcome) {
            case Created(var t) -> t.id();
            case Replayed(var t) -> t.id();
            case KeyMismatch _ -> throw new AssertionError("unexpected mismatch");
        };
    }

    @Test
    @DisplayName("a transfer against the id order waits for a transaction that locks in id order, without a deadlock")
    void idOrderNeverDeadlocks() throws Exception {
        var ids = fixtures.inIdOrder(fixtures.account(1_000), fixtures.account(1_000));
        var low = ids.getFirst();
        var high = ids.getLast();
        var deadlocksBefore = fixtures.deadlocks();

        var racer = fixtures.race(
                () -> fixtures.lock(low), () -> fixtures.lock(high), () -> fixtures.transfer(high, low, 1));

        assertThat(racer.get()).isInstanceOf(Created.class);
        assertThat(fixtures.deadlocks()).isEqualTo(deadlocksBefore);
    }

    @Test
    @DisplayName("a transfer that waits on a balance changed by another transaction posts on the new balance")
    void noLostUpdate() throws Exception {
        var from = fixtures.account(1_000);
        var to = fixtures.account(0);
        var other = fixtures.account(1_000);

        var racer =
                fixtures.race(() -> fixtures.transfer(other, to, 5), () -> {}, () -> fixtures.transfer(from, to, 1));

        assertThat(racer.get()).isInstanceOf(Created.class);
        assertThat(fixtures.balance(to)).isEqualTo(6).isEqualTo(fixtures.sumOfPostings(to));
    }
}
