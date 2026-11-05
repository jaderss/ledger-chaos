package dev.jaderss.projector;

import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

@Component
class BalanceProjection {

    private final JdbcClient jdbc;

    BalanceProjection(JdbcClient jdbc) {
        this.jdbc = jdbc;
    }

    @Transactional
    public boolean apply(TransferPosted event) {
        var firstTime = jdbc.sql("insert into processed_event (event_id) values (:id) on conflict do nothing")
                        .param("id", event.eventId())
                        .update()
                == 1;
        if (firstTime) {
            event.postingsInAccountOrder()
                    .forEach(line -> jdbc.sql("""
                            insert into balance (account_id, currency, balance_minor) values (:account, :currency, :amount)
                            on conflict (account_id) do update
                               set balance_minor = balance.balance_minor + excluded.balance_minor
                            """)
                            .param("account", line.accountId())
                            .param("currency", event.currency())
                            .param("amount", line.amountMinor())
                            .update());
        }
        return firstTime;
    }
}
