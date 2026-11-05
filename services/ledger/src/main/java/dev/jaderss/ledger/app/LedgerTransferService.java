package dev.jaderss.ledger.app;

import dev.jaderss.ledger.app.TransferOutcome.Created;
import dev.jaderss.ledger.app.TransferOutcome.KeyMismatch;
import dev.jaderss.ledger.app.TransferOutcome.Replayed;
import dev.jaderss.ledger.domain.Account;
import dev.jaderss.ledger.domain.AccountId;
import dev.jaderss.ledger.domain.AccountNotFoundException;
import dev.jaderss.ledger.domain.RejectReason;
import dev.jaderss.ledger.domain.RequestHash;
import dev.jaderss.ledger.domain.Transfer;
import dev.jaderss.ledger.domain.TransferCommand;
import dev.jaderss.ledger.domain.TransferDecision;
import dev.jaderss.ledger.domain.TransferDecision.Post;
import dev.jaderss.ledger.domain.TransferDecision.Reject;
import dev.jaderss.ledger.domain.TransferId;
import dev.jaderss.ledger.domain.TransferPosted;
import dev.jaderss.ledger.domain.TransferStatus;
import dev.jaderss.ledger.persistence.LedgerStores;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.stream.Stream;
import org.springframework.dao.ConcurrencyFailureException;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

final class LedgerTransferService implements TransferService {

    private final TransactionTemplate transaction;
    private final LedgerStores stores;
    private final TransferSteps steps;
    private final int retries;

    LedgerTransferService(
            PlatformTransactionManager transactionManager,
            LedgerStores stores,
            TransferSteps steps,
            TransactionPolicy policy) {
        this.transaction = new TransactionTemplate(transactionManager);
        this.transaction.setIsolationLevel(policy.isolationLevel());
        this.stores = stores;
        this.steps = steps;
        this.retries = policy.retries();
    }

    @Override
    public TransferOutcome transfer(TransferCommand command) {
        for (var attempt = 0; ; attempt++) {
            try {
                return Objects.requireNonNull(transaction.execute(_ -> transferInTransaction(command)));
            } catch (ConcurrencyFailureException e) {
                if (attempt >= retries) {
                    throw e;
                }
            }
        }
    }

    private TransferOutcome transferInTransaction(TransferCommand command) {
        var hash = command.hash();
        var id = TransferId.random();
        if (!steps.claim().claim(command.key(), hash, id)) {
            return replay(command, hash);
        }
        var locked = steps.load().load(command.from(), command.to());
        var from = pick(locked, command.from());
        var to = pick(locked, command.to());
        return new Created(record(id, command, TransferDecision.decide(from, to, command.amount())));
    }

    private TransferOutcome replay(TransferCommand command, RequestHash hash) {
        var entry = stores.idempotency()
                .find(command.key())
                .orElseThrow(() -> new IllegalStateException("idempotency key " + command.key() + " vanished"));
        if (!entry.hash().equals(hash)) {
            return new KeyMismatch(command.key());
        }
        return new Replayed(stores.transfers()
                .find(entry.transferId())
                .orElseThrow(() -> new IllegalStateException("transfer " + entry.transferId() + " vanished")));
    }

    private Transfer record(TransferId id, TransferCommand command, TransferDecision decision) {
        return switch (decision) {
            case Reject(var reason) ->
                stores.transfers().insert(newTransfer(id, command, TransferStatus.REJECTED, reason));
            case Post(var lines, var from, var to) -> {
                var transfer = stores.transfers().insert(newTransfer(id, command, TransferStatus.POSTED, null));
                stores.postings().insert(id, lines);
                Stream.of(from, to).sorted(Comparator.comparing(Account::id)).forEach(stores.accounts()::updateBalance);
                steps.publish().posted(TransferPosted.of(UUID.randomUUID(), transfer, lines));
                yield transfer;
            }
        };
    }

    private static Transfer newTransfer(
            TransferId id, TransferCommand command, TransferStatus status, RejectReason reason) {
        return new Transfer(
                id,
                command.key(),
                command.correlationId(),
                command.from(),
                command.to(),
                command.amount(),
                status,
                reason,
                null);
    }

    private static Account pick(List<Account> locked, AccountId id) {
        return locked.stream()
                .filter(account -> account.id().equals(id))
                .findFirst()
                .orElseThrow(() -> new AccountNotFoundException(id));
    }
}
