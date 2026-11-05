package dev.jaderss.checker;

import static org.assertj.core.api.Assertions.assertThat;

import dev.jaderss.checker.Snapshot.Account;
import dev.jaderss.checker.Snapshot.OutboxRow;
import dev.jaderss.checker.Snapshot.Transfer;
import java.util.Map;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class StoresTest {

    static final String A = "00000000-0000-0000-0000-00000000000a";
    static final String B = "00000000-0000-0000-0000-00000000000b";
    static final String T1 = "00000000-0000-0000-0000-0000000000f1";

    static final String LEDGER_SCHEMA = """
            create table account (id uuid primary key, currency char(3), allow_overdraft boolean, balance_minor bigint);
            create table transfer (id uuid primary key, request_key text, status text, from_account uuid, to_account uuid, amount_minor bigint);
            create table posting (transfer_id uuid, account_id uuid, amount_minor bigint);
            create table outbox (transfer_id uuid, published_at timestamptz);
            insert into account values ('%1$s', 'EUR', true, -40), ('%2$s', 'EUR', false, 40);
            insert into transfer values ('%3$s', 'k1', 'POSTED', '%1$s', '%2$s', 40);
            insert into posting values ('%3$s', '%1$s', -40), ('%3$s', '%2$s', 40);
            insert into outbox values ('%3$s', %4$s);
            """;

    static String ledgerSchema(boolean published) {
        return LEDGER_SCHEMA.formatted(A, B, T1, published ? "now()" : "null");
    }

    static String projectorSchema(long a, long b) {
        return """
                create table balance (account_id uuid primary key, balance_minor bigint);
                insert into balance values ('%s', %d), ('%s', %d);
                """.formatted(A, a, B, b);
    }

    static String event(String eventId) {
        return """
                {"eventId":"%s","transferId":"%s","postings":[{"accountId":"%s","amountMinor":-40},{"accountId":"%s","amountMinor":40}]}""".formatted(eventId, T1, A, B);
    }

    @Test
    @DisplayName("the ledger read fills every snapshot field")
    void loadsLedger() {
        var databases = Databases.connect(
                Containers.database(ledgerSchema(false)), Containers.database(projectorSchema(-7, 7)));

        var ledger = databases.loadLedger();

        assertThat(ledger.transfers()).containsExactly(new Transfer(T1, "k1", "POSTED", A, B, 40));
        assertThat(ledger.accounts()).containsExactlyInAnyOrder(new Account(A, true, -40), new Account(B, false, 40));
        assertThat(ledger.currencySums()).isEqualTo(Map.of("EUR", 0L));
        assertThat(ledger.postingSums()).isEqualTo(Map.of(A, -40L, B, 40L));
        assertThat(ledger.transferPostings()).isEqualTo(Map.of(T1, Map.of(A, -40L, B, 40L)));
        assertThat(ledger.outbox()).containsExactly(new OutboxRow(T1, false));
        assertThat(ledger.counters().commits()).isPositive();
        assertThat(databases.unpublished()).isOne();
        assertThat(databases.projectorBalances()).isEqualTo(Map.of(A, -7L, B, 7L));
    }

    @Test
    @DisplayName("the topic read gives events, the record count and the group's lag")
    void readsTopic() throws Exception {
        try (var admin = Containers.admin();
                var reader = new TopicReader(Containers.brokers(), Checker.json())) {
            var topic = Containers.topic(admin, 2);
            assertThat(reader.events(topic)).isEmpty();

            Containers.produce(topic, event("e0"), event("e1"), event("e2"));

            assertThat(reader.events(topic))
                    .hasSize(3)
                    .allSatisfy(e -> assertThat(e.postings()).containsEntry(B, 40L));
            assertThat(reader.count(topic)).isEqualTo(3);
            var group = topic + "-group";
            assertThat(reader.lag(group, topic)).isEqualTo(3);

            Containers.commitToEnd(admin, group, topic);
            assertThat(reader.lag(group, topic)).isZero();
        }
    }
}
