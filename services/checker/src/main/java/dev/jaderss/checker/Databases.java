package dev.jaderss.checker;

import dev.jaderss.checker.Config.Postgres;
import dev.jaderss.checker.Snapshot.Account;
import dev.jaderss.checker.Snapshot.Counters;
import dev.jaderss.checker.Snapshot.Ledger;
import dev.jaderss.checker.Snapshot.OutboxRow;
import dev.jaderss.checker.Snapshot.Transfer;
import java.util.HashMap;
import java.util.Map;
import org.jdbi.v3.core.Handle;
import org.jdbi.v3.core.Jdbi;
import org.jdbi.v3.postgres.PostgresPlugin;

record Databases(Jdbi ledger, Jdbi projector) {

    static Databases connect(Postgres ledger, Postgres projector) {
        return new Databases(jdbi(ledger), jdbi(projector));
    }

    private static Jdbi jdbi(Postgres postgres) {
        return Jdbi.create(postgres.url(), postgres.user(), postgres.password()).installPlugin(new PostgresPlugin());
    }

    Ledger loadLedger() {
        return ledger.withHandle(h -> new Ledger(
                h.createQuery(
                                "select id::text, request_key, status, from_account::text, to_account::text, amount_minor from transfer")
                        .map((rs, _) -> new Transfer(
                                rs.getString(1),
                                rs.getString(2),
                                rs.getString(3),
                                rs.getString(4),
                                rs.getString(5),
                                rs.getLong(6)))
                        .list(),
                h.createQuery("select id::text, allow_overdraft, balance_minor from account")
                        .map((rs, _) -> new Account(rs.getString(1), rs.getBoolean(2), rs.getLong(3)))
                        .list(),
                sums(h, """
                        select a.currency, sum(p.amount_minor)::bigint
                        from posting p join account a on a.id = p.account_id group by a.currency"""),
                sums(h, "select account_id::text, sum(amount_minor)::bigint from posting group by account_id"),
                transferPostings(h),
                h.createQuery("select transfer_id::text, published_at is not null from outbox")
                        .map((rs, _) -> new OutboxRow(rs.getString(1), rs.getBoolean(2)))
                        .list(),
                h.createQuery(
                                "select deadlocks, xact_commit, xact_rollback from pg_stat_database where datname = current_database()")
                        .map((rs, _) -> new Counters(rs.getLong(1), rs.getLong(2), rs.getLong(3)))
                        .one()));
    }

    long unpublished() {
        return ledger.withHandle(h -> h.createQuery("select count(*) from outbox where published_at is null")
                .mapTo(Long.class)
                .one());
    }

    Map<String, Long> projectorBalances() {
        return projector.withHandle(h -> sums(h, "select account_id::text, balance_minor from balance"));
    }

    private static Map<String, Long> sums(Handle h, String sql) {
        return h.createQuery(sql).reduceRows(new HashMap<>(), (sums, row) -> {
            sums.put(row.getColumn(1, String.class), row.getColumn(2, Long.class));
            return sums;
        });
    }

    private static Map<String, Map<String, Long>> transferPostings(Handle h) {
        return h.createQuery(
                        "select transfer_id::text, account_id::text, sum(amount_minor)::bigint from posting group by 1, 2")
                .reduceRows(new HashMap<>(), (postings, row) -> {
                    postings.computeIfAbsent(row.getColumn(1, String.class), _ -> new HashMap<>())
                            .put(row.getColumn(2, String.class), row.getColumn(3, Long.class));
                    return postings;
                });
    }
}
