package dev.jaderss.checker;

import dev.jaderss.checker.History.Answer;
import dev.jaderss.checker.History.Body;
import dev.jaderss.checker.History.KeyHistory;
import dev.jaderss.checker.History.MismatchAttempt;
import dev.jaderss.checker.Snapshot.Account;
import dev.jaderss.checker.Snapshot.Counters;
import dev.jaderss.checker.Snapshot.Event;
import dev.jaderss.checker.Snapshot.Ledger;
import dev.jaderss.checker.Snapshot.OutboxRow;
import dev.jaderss.checker.Snapshot.Quiet;
import dev.jaderss.checker.Snapshot.Topic;
import dev.jaderss.checker.Snapshot.Transfer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

final class SnapshotFixture {

    static final String ALICE = "a1";
    static final String BOB = "b2";
    static final String MINT = "m0";

    Mode mode = Mode.CORRECT;
    final Map<String, KeyHistory> keys = new HashMap<>();
    final List<Transfer> transfers = new ArrayList<>();
    final List<Account> accounts = new ArrayList<>();
    final Map<String, Long> currencySums = new HashMap<>();
    final Map<String, Long> postingSums = new HashMap<>();
    final Map<String, Map<String, Long>> transferPostings = new HashMap<>();
    final List<OutboxRow> outbox = new ArrayList<>();
    long deadlocks;
    final List<Event> events = new ArrayList<>();
    long deadLetters;
    final Map<String, Long> projectorBalances = new HashMap<>();
    Quiet quiet = new Quiet(true, "");

    static SnapshotFixture clean() {
        var f = new SnapshotFixture();
        f.key("fund", new Body(MINT, ALICE, 100, "EUR"), answer("ok", 201, "t1", "POSTED"));
        f.key("pay", new Body(ALICE, BOB, 30, "EUR"), Answer.NONE, new Answer("ok", 200, "t2", "POSTED", null, true));
        f.key(
                "too-much",
                new Body(BOB, ALICE, 999, "EUR"),
                new Answer("fail", 422, "t3", "REJECTED", "insufficient-funds", false));
        f.key("lost", new Body(ALICE, BOB, 5, "EUR"), Answer.NONE, Answer.NONE);
        f.transfers.addAll(List.of(
                new Transfer("t1", "fund", "POSTED", MINT, ALICE, 100),
                new Transfer("t2", "pay", "POSTED", ALICE, BOB, 30),
                new Transfer("t3", "too-much", "REJECTED", BOB, ALICE, 999)));
        f.accounts.addAll(
                List.of(new Account(MINT, true, -100), new Account(ALICE, false, 70), new Account(BOB, false, 30)));
        f.currencySums.put("EUR", 0L);
        f.postingSums.putAll(Map.of(MINT, -100L, ALICE, 70L, BOB, 30L));
        f.transferPostings.put("t1", new HashMap<>(Map.of(MINT, -100L, ALICE, 100L)));
        f.transferPostings.put("t2", new HashMap<>(Map.of(ALICE, -30L, BOB, 30L)));
        f.outbox.addAll(List.of(new OutboxRow("t1", true), new OutboxRow("t2", true)));
        f.events.addAll(List.of(
                new Event("e1", "t1", Map.of(MINT, -100L, ALICE, 100L)),
                new Event("e2", "t2", Map.of(ALICE, -30L, BOB, 30L)),
                new Event("e2", "t2", Map.of(ALICE, -30L, BOB, 30L))));
        f.projectorBalances.putAll(Map.of(MINT, -100L, ALICE, 70L, BOB, 30L));
        return f;
    }

    static Answer answer(String phase, int status, String transferId, String transferStatus) {
        return new Answer(phase, status, transferId, transferStatus, null, false);
    }

    void key(String key, Body body, Answer... answers) {
        keys.put(key, new KeyHistory(key, body, new ArrayList<>(List.of(answers)), new ArrayList<>()));
    }

    void addAnswer(String key, Answer answer) {
        keys.get(key).answers().add(answer);
    }

    void addMismatch(String key, Body body, Answer answer) {
        keys.get(key).mismatches().add(new MismatchAttempt(body, answer));
    }

    void addPostedTransfer(String id, String key) {
        transfers.add(new Transfer(id, key, "POSTED", "", "", 0));
    }

    Snapshot build() {
        return new Snapshot(
                mode,
                keys,
                new Ledger(
                        transfers,
                        accounts,
                        currencySums,
                        postingSums,
                        transferPostings,
                        outbox,
                        new Counters(deadlocks, 0, 0)),
                new Topic(events, deadLetters),
                projectorBalances,
                quiet);
    }

    List<String> failing() {
        return Invariants.check(build()).stream()
                .filter(r -> !r.pass())
                .map(Result::id)
                .toList();
    }
}
