package dev.jaderss.ledger.persistence;

import dev.jaderss.ledger.domain.Account;
import dev.jaderss.ledger.domain.AccountId;
import dev.jaderss.ledger.domain.Money;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Stream;
import org.jdbi.v3.core.Handle;
import org.jdbi.v3.core.statement.StatementContext;

public record AccountStore(Handle handle) {

    private static final String COLUMNS = "select id, currency, allow_overdraft, balance_minor from account ";

    public boolean create(Account account) {
        return handle.createUpdate("""
                        insert into account (id, currency, allow_overdraft) values (:id, :currency, :overdraft)
                        on conflict (id) do nothing
                        """)
                        .bind("id", account.id().value())
                        .bind("currency", account.balance().currency().getCurrencyCode())
                        .bind("overdraft", account.allowOverdraft())
                        .execute()
                == 1;
    }

    public Optional<Account> find(AccountId id) {
        return handle.createQuery(COLUMNS + "where id = :id")
                .bind("id", id.value())
                .map(AccountStore::map)
                .findOne();
    }

    public List<Account> lockInIdOrder(AccountId first, AccountId second) {
        return handle.createQuery(COLUMNS + "where id in (:first, :second) order by id for update")
                .bind("first", first.value())
                .bind("second", second.value())
                .map(AccountStore::map)
                .list();
    }

    public List<Account> lockInRequestOrder(AccountId from, AccountId to) {
        return Stream.concat(lock(from).stream(), lock(to).stream()).toList();
    }

    public List<Account> readWithoutLock(AccountId first, AccountId second) {
        return handle.createQuery(COLUMNS + "where id in (:first, :second)")
                .bind("first", first.value())
                .bind("second", second.value())
                .map(AccountStore::map)
                .list();
    }

    private Optional<Account> lock(AccountId id) {
        return handle.createQuery(COLUMNS + "where id = :id for update")
                .bind("id", id.value())
                .map(AccountStore::map)
                .findOne();
    }

    public void updateBalance(Account account) {
        handle.createUpdate("update account set balance_minor = :balance where id = :id")
                .bind("balance", account.balance().amountMinor())
                .bind("id", account.id().value())
                .execute();
    }

    private static Account map(ResultSet rs, StatementContext context) throws SQLException {
        return new Account(
                new AccountId(rs.getObject("id", UUID.class)),
                Money.of(rs.getLong("balance_minor"), rs.getString("currency")),
                rs.getBoolean("allow_overdraft"));
    }
}
