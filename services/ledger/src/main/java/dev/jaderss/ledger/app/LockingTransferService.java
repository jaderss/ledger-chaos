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
import dev.jaderss.ledger.persistence.AccountStore;
import dev.jaderss.ledger.persistence.IdempotencyStore;
import dev.jaderss.ledger.persistence.OutboxStore;
import dev.jaderss.ledger.persistence.PostingStore;
import dev.jaderss.ledger.persistence.TransferStore;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.support.TransactionTemplate;

final class LockingTransferService implements TransferService {

    private final TransactionTemplate transaction;
    private final IdempotencyStore idempotency;
    private final AccountStore accounts;
    private final TransferStore transfers;
    private final PostingStore postings;
    private final OutboxStore outbox;

    LockingTransferService(
            PlatformTransactionManager transactionManager,
            IdempotencyStore idempotency,
            AccountStore accounts,
            TransferStore transfers,
            PostingStore postings,
            OutboxStore outbox) {
        this.transaction = new TransactionTemplate(transactionManager);
        this.transaction.setIsolationLevel(TransactionDefinition.ISOLATION_READ_COMMITTED);
        this.idempotency = idempotency;
        this.accounts = accounts;
        this.transfers = transfers;
        this.postings = postings;
        this.outbox = outbox;
    }

    @Override
    public TransferOutcome transfer(TransferCommand command) {
        return Objects.requireNonNull(transaction.execute(_ -> transferInTransaction(command)));
    }

    private TransferOutcome transferInTransaction(TransferCommand command) {
        var hash = command.hash();
        var id = TransferId.random();
        if (!idempotency.claim(command.key(), hash, id)) {
            return replay(command, hash);
        }
        var locked = accounts.lockInIdOrder(command.from(), command.to());
        var from = pick(locked, command.from());
        var to = pick(locked, command.to());
        return new Created(record(id, command, TransferDecision.decide(from, to, command.amount())));
    }

    private TransferOutcome replay(TransferCommand command, RequestHash hash) {
        var entry = idempotency
                .find(command.key())
                .orElseThrow(() -> new IllegalStateException("idempotency key " + command.key() + " vanished"));
        if (!entry.hash().equals(hash)) {
            return new KeyMismatch(command.key());
        }
        return new Replayed(transfers
                .find(entry.transferId())
                .orElseThrow(() -> new IllegalStateException("transfer " + entry.transferId() + " vanished")));
    }

    private Transfer record(TransferId id, TransferCommand command, TransferDecision decision) {
        return switch (decision) {
            case Reject(var reason) -> transfers.insert(newTransfer(id, command, TransferStatus.REJECTED, reason));
            case Post(var lines, var from, var to) -> {
                var transfer = transfers.insert(newTransfer(id, command, TransferStatus.POSTED, null));
                postings.insert(id, lines);
                accounts.updateBalance(from);
                accounts.updateBalance(to);
                outbox.append(TransferPosted.of(UUID.randomUUID(), transfer, lines));
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
