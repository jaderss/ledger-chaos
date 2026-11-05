package dev.jaderss.checker;

import static java.util.stream.Collectors.counting;
import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.toMap;

import dev.jaderss.checker.History.Answer;
import dev.jaderss.checker.History.KeyHistory;
import dev.jaderss.checker.Result.Violations;
import dev.jaderss.checker.Snapshot.Transfer;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.function.Function;

final class Invariants {

    private static final String MISMATCH_PROBLEM = "idempotency-key-mismatch";

    private final Snapshot snapshot;
    private final Map<String, List<Transfer>> byKey;

    private Invariants(Snapshot snapshot) {
        this.snapshot = snapshot;
        this.byKey = snapshot.ledger().transfers().stream().collect(groupingBy(Transfer::key));
    }

    static List<Result> check(Snapshot snapshot) {
        var invariants = new Invariants(snapshot);
        return List.of(
                invariants.i1(),
                invariants.i2(),
                invariants.i3(),
                invariants.i4(),
                invariants.i5(),
                invariants.i6(),
                invariants.i7(),
                invariants.i8(),
                invariants.i9(),
                invariants.i11());
    }

    private List<Transfer> transfersOf(String key) {
        return byKey.getOrDefault(key, List.of());
    }

    private long postedOf(String key) {
        return transfersOf(key).stream().filter(Transfer::posted).count();
    }

    private Map<String, KeyHistory> sortedKeys() {
        return new TreeMap<>(snapshot.keys());
    }

    private long postingSum(String account) {
        return snapshot.ledger().postingSums().getOrDefault(account, 0L);
    }

    private Result i1() {
        var v = new Violations();
        new TreeMap<>(snapshot.ledger().currencySums()).forEach((currency, sum) -> {
            if (sum != 0) {
                v.add("%s postings sum to %d", currency, sum);
            }
        });
        return v.result("I1", "sum of all postings per currency is zero");
    }

    private Result i2() {
        var v = new Violations();
        for (var a : snapshot.ledger().accounts()) {
            if (a.balance() != postingSum(a.id())) {
                v.add("account %s stores %d, postings sum to %d", a.id(), a.balance(), postingSum(a.id()));
            }
        }
        return v.result("I2", "each account balance equals the sum of its postings");
    }

    private Result i3() {
        var v = new Violations();
        for (var a : snapshot.ledger().accounts()) {
            if (!a.overdraft() && postingSum(a.id()) < 0) {
                v.add("account %s without overdraft has postings summing to %d", a.id(), postingSum(a.id()));
            }
        }
        return v.result("I3", "no account without overdraft goes below zero");
    }

    private Result i4() {
        var v = new Violations();
        sortedKeys().forEach((key, history) -> {
            if (history.posted() && postedOf(key) != 1) {
                v.add("key %s was acknowledged and has %d posted transfers", key, postedOf(key));
            }
        });
        return v.result("I4", "an acknowledged transfer exists exactly once");
    }

    private Result i5() {
        var v = new Violations();
        sortedKeys().forEach((key, history) -> {
            var definite = history.definite();
            if (!history.posted() && !definite.isEmpty() && postedOf(key) != 0) {
                v.add(
                        "key %s was refused with %d and has %d posted transfers",
                        key, definite.getFirst().status(), postedOf(key));
            }
        });
        return v.result("I5", "a transfer refused with a definite error does not exist");
    }

    private Result i6() {
        var v = new Violations();
        sortedKeys().forEach((key, history) -> {
            var onlyUnknown = !history.answers().isEmpty() && history.definite().isEmpty();
            if (onlyUnknown && transfersOf(key).size() > 1) {
                v.add(
                        "key %s had only unknown outcomes and has %d transfers",
                        key, transfersOf(key).size());
            }
        });
        return v.result("I6", "a transfer with unknown outcome exists at most once");
    }

    private Result i7() {
        var v = new Violations();
        sortedKeys().forEach((key, history) -> {
            var identified = history.definite().stream()
                    .filter(a -> a.transferId() != null && !a.transferId().isEmpty())
                    .toList();
            if (!identified.isEmpty()) {
                var first = identified.getFirst();
                for (var a : identified.subList(1, identified.size())) {
                    if (!a.transferId().equals(first.transferId())
                            || !Objects.equals(a.transferStatus(), first.transferStatus())) {
                        v.add(
                                "key %s answered %s %s, then %s %s",
                                key, first.transferId(), first.transferStatus(), a.transferId(), a.transferStatus());
                    }
                }
            }
            for (var m : history.mismatches()) {
                Answer answer = m.answer();
                if (answer.definite() && !MISMATCH_PROBLEM.equals(answer.problem())) {
                    v.add("key %s reused with another body answered %d %s", key, answer.status(), answer.problem());
                }
                for (var t : transfersOf(key)) {
                    if (t.from().equals(m.body().from())
                            && t.to().equals(m.body().to())
                            && t.amount() == m.body().amount()) {
                        v.add("key %s reused with another body created transfer %s", key, t.id());
                    }
                }
            }
        });
        return v.result("I7", "same key and body give the same answer, another body changes nothing");
    }

    private void quiet(Violations v) {
        if (!snapshot.quiet().reached()) {
            v.add("system never got quiet: %s", snapshot.quiet().detail());
        }
    }

    private Result i8() {
        var v = new Violations();
        quiet(v);
        var transfers =
                snapshot.ledger().transfers().stream().collect(toMap(Transfer::id, Function.identity(), (a, _) -> a));
        Set<String> onTopic = new TreeSet<>();
        for (var e : snapshot.topic().events()) {
            onTopic.add(e.transferId());
            var t = transfers.get(e.transferId());
            if (t == null || !t.posted()) {
                v.add("event %s for transfer %s has no posted transfer", e.eventId(), e.transferId());
                continue;
            }
            var ledgerPostings = snapshot.ledger().transferPostings().getOrDefault(e.transferId(), Map.of());
            if (!e.postings().equals(ledgerPostings)) {
                v.add(
                        "event %s postings %s differ from the ledger's %s",
                        e.eventId(), new TreeMap<>(e.postings()), new TreeMap<>(ledgerPostings));
            }
        }
        for (var t : snapshot.ledger().transfers()) {
            if (t.posted() && !onTopic.contains(t.id())) {
                v.add("posted transfer %s is not on the topic", t.id());
            }
        }
        if (snapshot.mode() == Mode.CORRECT) {
            var outbox = snapshot.ledger().outbox();
            for (var o : outbox) {
                if (!o.published()) {
                    v.add("outbox row for transfer %s is unpublished", o.transferId());
                }
            }
            var rows = outbox.stream().collect(groupingBy(Snapshot.OutboxRow::transferId, counting()));
            for (var t : snapshot.ledger().transfers()) {
                var count = rows.getOrDefault(t.id(), 0L);
                if (t.posted() && count != 1) {
                    v.add("posted transfer %s has %d outbox rows", t.id(), count);
                }
            }
        }
        return v.result("I8", "every posted transfer reaches the topic, and only those");
    }

    private Result i9() {
        var v = new Violations();
        quiet(v);
        if (snapshot.topic().deadLetters() > 0) {
            v.add("%d records in the DLT", snapshot.topic().deadLetters());
        }
        var accounts = new TreeSet<>(snapshot.ledger().postingSums().keySet());
        accounts.addAll(snapshot.projectorBalances().keySet());
        for (var id : accounts) {
            var projected = snapshot.projectorBalances().getOrDefault(id, 0L);
            if (projected != postingSum(id)) {
                v.add("account %s projected %d, ledger postings sum to %d", id, projected, postingSum(id));
            }
        }
        return v.result("I9", "projector balances equal the ledger's sums of postings");
    }

    private Result i11() {
        var v = new Violations();
        var deadlocks = snapshot.ledger().counters().deadlocks();
        if (deadlocks > 0) {
            v.add("%d deadlocks in the ledger database", deadlocks);
        }
        return v.result("I11", "no transfer transaction ends in a deadlock");
    }
}
