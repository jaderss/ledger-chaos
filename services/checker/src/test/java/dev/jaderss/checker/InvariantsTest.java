package dev.jaderss.checker;

import static dev.jaderss.checker.SnapshotFixture.ALICE;
import static dev.jaderss.checker.SnapshotFixture.BOB;
import static dev.jaderss.checker.SnapshotFixture.MINT;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.InstanceOfAssertFactories.LIST;
import static org.junit.jupiter.params.provider.Arguments.argumentSet;

import dev.jaderss.checker.History.Answer;
import dev.jaderss.checker.History.Body;
import dev.jaderss.checker.Snapshot.Account;
import dev.jaderss.checker.Snapshot.Event;
import dev.jaderss.checker.Snapshot.OutboxRow;
import dev.jaderss.checker.Snapshot.Quiet;
import dev.jaderss.checker.Snapshot.Transfer;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class InvariantsTest {

    @Test
    @DisplayName("a clean snapshot passes every invariant")
    void cleanPasses() {
        assertThat(SnapshotFixture.clean().failing()).isEmpty();
    }

    static Stream<Arguments> corruptions() {
        return Stream.of(
                corruption("postings of a currency do not sum to zero", f -> f.currencySums.put("EUR", 7L), "I1"),
                corruption(
                        "stored balance differs from postings",
                        f -> f.accounts.set(1, new Account(ALICE, false, 71)),
                        "I2"),
                corruption(
                        "account without overdraft overspent",
                        f -> {
                            f.postingSums.put(BOB, -1L);
                            f.accounts.set(2, new Account(BOB, false, -1));
                            f.projectorBalances.put(BOB, -1L);
                        },
                        "I3"),
                corruption(
                        "acknowledged key posted twice",
                        f -> {
                            f.addPostedTransfer("t9", "pay");
                            f.outbox.add(new OutboxRow("t9", true));
                            f.events.add(new Event("e9", "t9", Map.of()));
                        },
                        "I4"),
                corruption(
                        "refused key posted",
                        f -> {
                            f.transfers.set(2, new Transfer("t3", "too-much", "POSTED", BOB, ALICE, 999));
                            f.outbox.add(new OutboxRow("t3", true));
                            f.events.add(new Event("e3", "t3", Map.of()));
                        },
                        "I5"),
                corruption(
                        "unknown key written twice",
                        f -> f.transfers.addAll(List.of(
                                new Transfer("t7", "lost", "REJECTED", "", "", 0),
                                new Transfer("t8", "lost", "REJECTED", "", "", 0))),
                        "I6"),
                corruption(
                        "replay answered another transfer",
                        f -> f.addAnswer("pay", new Answer("ok", 200, "t5", "POSTED", null, false)),
                        "I7"),
                corruption(
                        "mismatch body accepted",
                        f -> f.addMismatch(
                                "fund",
                                new Body(MINT, ALICE, 101, "EUR"),
                                new Answer("ok", 201, null, null, null, false)),
                        "I7"),
                corruption(
                        "mismatch body created a transfer",
                        f -> {
                            f.addMismatch("fund", new Body(MINT, ALICE, 101, "EUR"), Answer.NONE);
                            f.transfers.add(new Transfer("t6", "fund", "REJECTED", MINT, ALICE, 101));
                        },
                        "I7"),
                corruption(
                        "posted transfer missing from the topic",
                        f -> f.events.subList(1, f.events.size()).clear(),
                        "I8"),
                corruption(
                        "event with other postings than the ledger",
                        f -> f.events.set(0, new Event("e1", "t1", Map.of(MINT, -101L, ALICE, 101L))),
                        "I8"),
                corruption("phantom event", f -> f.events.add(new Event("x", "nope", Map.of())), "I8"),
                corruption("outbox row left unpublished", f -> f.outbox.set(0, new OutboxRow("t1", false)), "I8"),
                corruption("projector off by one", f -> f.projectorBalances.put(ALICE, 69L), "I9"),
                corruption(
                        "projector knows an account the ledger does not",
                        f -> f.projectorBalances.put("ghost", 5L),
                        "I9"),
                corruption("record in the DLT", f -> f.deadLetters = 1, "I9"),
                corruption("never quiet", f -> f.quiet = new Quiet(false, "lag 3"), "I8", "I9"),
                corruption("deadlock", f -> f.deadlocks = 2, "I11"));
    }

    private static Arguments corruption(String name, Consumer<SnapshotFixture> corrupt, String... failing) {
        return argumentSet(name, corrupt, List.of(failing));
    }

    @ParameterizedTest
    @MethodSource("corruptions")
    @DisplayName("each corruption fails exactly its invariant")
    void corruptionFailsItsInvariant(Consumer<SnapshotFixture> corrupt, List<String> failing) {
        var fixture = SnapshotFixture.clean();
        corrupt.accept(fixture);

        assertThat(fixture.failing()).isEqualTo(failing);
    }

    @Test
    @DisplayName("outbox rows are only required in the correct mode")
    void outboxOnlyInCorrectMode() {
        var fixture = SnapshotFixture.clean();
        fixture.mode = Mode.DUAL_WRITE;
        fixture.outbox.clear();

        assertThat(fixture.failing()).isEmpty();
    }

    @Test
    @DisplayName("examples are capped at five, the count is not")
    void examplesCapped() {
        var violations = new Result.Violations();
        IntStream.range(0, 8).forEach(i -> violations.add("violation %d", i));

        assertThat(violations.result("I0", "rule"))
                .returns(8, Result::count)
                .returns(false, Result::pass)
                .extracting(Result::examples)
                .asInstanceOf(LIST)
                .hasSize(5);
    }
}
