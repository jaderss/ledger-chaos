package dev.jaderss.ledger;

import static org.awaitility.Awaitility.await;

import dev.jaderss.ledger.app.TransferOutcome;
import dev.jaderss.ledger.app.TransferService;
import dev.jaderss.ledger.domain.Account;
import dev.jaderss.ledger.domain.AccountId;
import dev.jaderss.ledger.domain.IdempotencyKey;
import dev.jaderss.ledger.domain.Money;
import dev.jaderss.ledger.domain.TransferCommand;
import dev.jaderss.ledger.persistence.AccountStore;
import java.time.Duration;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicReference;
import org.springframework.dao.PessimisticLockingFailureException;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Component;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

@Component
public class Fixtures {

    private final AccountStore accounts;
    private final TransferService transfers;
    private final JdbcClient jdbc;
    private final TransactionTemplate transaction;
    private final AccountId mint = new AccountId(UUID.randomUUID());

    Fixtures(
            AccountStore accounts,
            TransferService transfers,
            JdbcClient jdbc,
            PlatformTransactionManager transactionManager) {
        this.accounts = accounts;
        this.transfers = transfers;
        this.jdbc = jdbc;
        this.transaction = new TransactionTemplate(transactionManager);
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

    public List<AccountId> inIdOrder(AccountId first, AccountId second) {
        return jdbc
                .sql("select id from account where id in (:first, :second) order by id")
                .param("first", first.value())
                .param("second", second.value())
                .query(UUID.class)
                .list()
                .stream()
                .map(AccountId::new)
                .toList();
    }

    public void lock(AccountId id) {
        jdbc.sql("select id from account where id = :id for update")
                .param("id", id.value())
                .query(UUID.class)
                .single();
    }

    public long deadlocks() {
        return jdbc.sql("select deadlocks from pg_stat_database where datname = current_database()")
                .query(Long.class)
                .single();
    }

    public Future<TransferOutcome> race(Runnable holding, Runnable thenTaking, Callable<TransferOutcome> racer) {
        try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
            var racing = new AtomicReference<Future<TransferOutcome>>();
            try {
                transaction.executeWithoutResult(_ -> {
                    holding.run();
                    racing.set(executor.submit(racer));
                    await().atMost(Duration.ofSeconds(10)).until(this::someoneWaitsOnALock);
                    thenTaking.run();
                });
            } catch (PessimisticLockingFailureException _) {
                return racing.get();
            }
            return racing.get();
        }
    }

    private boolean someoneWaitsOnALock() {
        return jdbc.sql("""
                select exists (select 1 from pg_stat_activity
                                where datname = current_database() and wait_event_type = 'Lock')
                """).query(Boolean.class).single();
    }
}
