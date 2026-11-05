package dev.jaderss.ledger.persistence;

import dev.jaderss.ledger.domain.Account;
import dev.jaderss.ledger.domain.AccountId;
import dev.jaderss.ledger.domain.Money;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;

@Repository
public class AccountStore {

    private static final String COLUMNS = "select id, currency, allow_overdraft, balance_minor from account ";

    private final JdbcClient jdbc;

    public AccountStore(JdbcClient jdbc) {
        this.jdbc = jdbc;
    }

    public boolean create(Account account) {
        return jdbc.sql("""
                insert into account (id, currency, allow_overdraft) values (:id, :currency, :overdraft)
                on conflict (id) do nothing
                """)
                        .param("id", account.id().value())
                        .param("currency", account.balance().currency().getCurrencyCode())
                        .param("overdraft", account.allowOverdraft())
                        .update()
                == 1;
    }

    public Optional<Account> find(AccountId id) {
        return jdbc.sql(COLUMNS + "where id = :id")
                .param("id", id.value())
                .query(AccountStore::map)
                .optional();
    }

    public List<Account> lockInIdOrder(AccountId first, AccountId second) {
        return jdbc.sql(COLUMNS + "where id in (:first, :second) order by id for update")
                .param("first", first.value())
                .param("second", second.value())
                .query(AccountStore::map)
                .list();
    }

    public void updateBalance(Account account) {
        jdbc.sql("update account set balance_minor = :balance where id = :id")
                .param("balance", account.balance().amountMinor())
                .param("id", account.id().value())
                .update();
    }

    private static Account map(ResultSet rs, int row) throws SQLException {
        return new Account(
                new AccountId(rs.getObject("id", UUID.class)),
                Money.of(rs.getLong("balance_minor"), rs.getString("currency")),
                rs.getBoolean("allow_overdraft"));
    }
}
