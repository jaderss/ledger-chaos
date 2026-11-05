package dev.jaderss.projector;

import org.jdbi.v3.core.Jdbi;
import org.springframework.stereotype.Component;

@Component
class BalanceProjection {

    private final Jdbi jdbi;

    BalanceProjection(Jdbi jdbi) {
        this.jdbi = jdbi;
    }

    public boolean apply(TransferPosted event) {
        return jdbi.inTransaction(handle -> {
            var firstTime =
                    handle.createUpdate("insert into processed_event (event_id) values (:id) on conflict do nothing")
                                    .bind("id", event.eventId())
                                    .execute()
                            == 1;
            if (firstTime) {
                event.postingsInAccountOrder()
                        .forEach(line -> handle.createUpdate("""
                                        insert into balance (account_id, currency, balance_minor)
                                        values (:account, :currency, :amount)
                                        on conflict (account_id) do update
                                           set balance_minor = balance.balance_minor + excluded.balance_minor
                                        """)
                                .bind("account", line.accountId())
                                .bind("currency", event.currency())
                                .bind("amount", line.amountMinor())
                                .execute());
            }
            return firstTime;
        });
    }
}
