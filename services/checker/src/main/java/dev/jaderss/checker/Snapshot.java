package dev.jaderss.checker;

import dev.jaderss.checker.History.KeyHistory;
import java.util.List;
import java.util.Map;

record Snapshot(
        Mode mode,
        Map<String, KeyHistory> keys,
        Ledger ledger,
        Topic topic,
        Map<String, Long> projectorBalances,
        Quiet quiet) {

    static final String POSTED = "POSTED";

    record Transfer(String id, String key, String status, String from, String to, long amount) {

        boolean posted() {
            return POSTED.equals(status);
        }
    }

    record Account(String id, boolean overdraft, long balance) {}

    record OutboxRow(String transferId, boolean published) {}

    record Counters(long deadlocks, long commits, long rollbacks) {}

    record Ledger(
            List<Transfer> transfers,
            List<Account> accounts,
            Map<String, Long> currencySums,
            Map<String, Long> postingSums,
            Map<String, Map<String, Long>> transferPostings,
            List<OutboxRow> outbox,
            Counters counters) {}

    record Event(String eventId, String transferId, Map<String, Long> postings) {}

    record Topic(List<Event> events, long deadLetters) {}

    record Quiet(boolean reached, String detail) {}
}
