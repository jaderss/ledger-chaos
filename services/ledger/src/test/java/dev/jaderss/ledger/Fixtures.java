package dev.jaderss.ledger;

import dev.jaderss.ledger.app.TransferOutcome;
import dev.jaderss.ledger.app.TransferService;
import dev.jaderss.ledger.domain.Account;
import dev.jaderss.ledger.domain.AccountId;
import dev.jaderss.ledger.domain.IdempotencyKey;
import dev.jaderss.ledger.domain.Money;
import dev.jaderss.ledger.domain.TransferCommand;
import dev.jaderss.ledger.persistence.AccountStore;
import java.util.UUID;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Component;

@Component
public class Fixtures {

    private final AccountStore accounts;
    private final TransferService transfers;
    private final JdbcClient jdbc;
    private final AccountId mint = new AccountId(UUID.randomUUID());

    Fixtures(AccountStore accounts, TransferService transfers, JdbcClient jdbc) {
        this.accounts = accounts;
        this.transfers = transfers;
        this.jdbc = jdbc;
        accounts.create(new Account(mint, Money.of(0, "EUR"), true));
    }

    public AccountId account(long funding) {
        var id = new AccountId(UUID.randomUUID());
        accounts.create(new Account(id, Money.of(0, "EUR"), false));
        if (funding > 0) {
            transfers.transfer(command(mint, id, funding));
        }
        return id;
    }

    public TransferOutcome transfer(AccountId from, AccountId to, long amount) {
        return transfers.transfer(command(from, to, amount));
    }

    public static TransferCommand command(AccountId from, AccountId to, long amount) {
        return new TransferCommand(
                new IdempotencyKey(UUID.randomUUID().toString()), "test", from, to, Money.of(amount, "EUR"));
    }

    public long balance(AccountId id) {
        return accounts.find(id).orElseThrow().balance().amountMinor();
    }

    public long sumOfPostings(AccountId id) {
        return jdbc.sql("select coalesce(sum(amount_minor), 0) from posting where account_id = :id")
                .param("id", id.value())
                .query(Long.class)
                .single();
    }
}
