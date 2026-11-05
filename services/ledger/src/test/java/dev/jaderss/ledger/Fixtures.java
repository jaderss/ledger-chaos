package dev.jaderss.ledger;

import static org.awaitility.Awaitility.await;

import dev.jaderss.ledger.app.TransferOutcome;
import dev.jaderss.ledger.app.TransferService;
import dev.jaderss.ledger.domain.Account;
import dev.jaderss.ledger.domain.AccountId;
import dev.jaderss.ledger.domain.IdempotencyKey;
import dev.jaderss.ledger.domain.Money;
import dev.jaderss.ledger.domain.TransferCommand;
import dev.jaderss.ledger.persistence.DatabaseException.ConcurrencyConflict;
import dev.jaderss.ledger.persistence.Isolation;
import dev.jaderss.ledger.persistence.LedgerDatabase;
import java.time.Duration;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import org.jdbi.v3.core.Handle;
import org.springframework.stereotype.Component;

@Component
public class Fixtures {

    private final LedgerDatabase database;
    private final TransferService transfers;
    private final AccountId mint = new AccountId(UUID.randomUUID());

    Fixtures(LedgerDatabase database, TransferService transfers) {
        this.database = database;
        this.transfers = transfers;
        database.withStores(stores -> stores.accounts().create(new Account(mint, Money.of(0, "EUR"), true)));
    }

    public <T> T sql(Function<Handle, T> query) {
        return database.withStores(stores -> query.apply(stores.handle()));
    }

    public AccountId account(long funding) {
        var id = new AccountId(UUID.randomUUID());
        database.withStores(stores -> stores.accounts().create(new Account(id, Money.of(0, "EUR"), false)));
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
        return database.withStores(stores -> stores.accounts().find(id))
                .orElseThrow()
                .balance()
                .amountMinor();
    }

    public long sumOfPostings(AccountId id) {
        return sql(h -> h.createQuery("select coalesce(sum(amount_minor), 0) from posting where account_id = :id")
                .bind("id", id.value())
                .mapTo(Long.class)
                .one());
    }

    public List<AccountId> inIdOrder(AccountId first, AccountId second) {
        return sql(h -> h.createQuery("select id from account where id in (:first, :second) order by id")
                        .bind("first", first.value())
                        .bind("second", second.value())
                        .mapTo(UUID.class)
                        .list())
                .stream()
                .map(AccountId::new)
                .toList();
    }

    public void lock(AccountId id) {
        sql(h -> h.createQuery("select id from account where id = :id for update")
                .bind("id", id.value())
                .mapTo(UUID.class)
                .one());
    }

    public long deadlocks() {
        return sql(h -> h.createQuery("select deadlocks from pg_stat_database where datname = current_database()")
                .mapTo(Long.class)
                .one());
    }

    public Future<TransferOutcome> race(Runnable holding, Runnable thenTaking, Callable<TransferOutcome> racer) {
        try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
            var racing = new AtomicReference<Future<TransferOutcome>>();
            try {
                database.inTransaction(Isolation.READ_COMMITTED, _ -> {
                    holding.run();
                    racing.set(executor.submit(racer));
                    await().atMost(Duration.ofSeconds(10)).until(this::someoneWaitsOnALock);
                    thenTaking.run();
                    return null;
                });
            } catch (ConcurrencyConflict _) {
                return racing.get();
            }
            return racing.get();
        }
    }

    private boolean someoneWaitsOnALock() {
        return sql(h -> h.createQuery("""
                        select exists (select 1 from pg_stat_activity
                                        where datname = current_database() and wait_event_type = 'Lock')
                        """).mapTo(Boolean.class).one());
    }
}
