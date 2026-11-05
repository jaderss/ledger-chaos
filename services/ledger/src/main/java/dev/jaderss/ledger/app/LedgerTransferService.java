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
import dev.jaderss.ledger.persistence.DatabaseException.ConcurrencyConflict;
import dev.jaderss.ledger.persistence.LedgerDatabase;
import dev.jaderss.ledger.persistence.LedgerStores;
import java.util.Comparator;
import java.util.List;
import java.util.UUID;
import java.util.stream.Stream;

final class LedgerTransferService implements TransferService {

    private final LedgerDatabase database;
    private final TransferSteps steps;
    private final TransactionPolicy policy;

    LedgerTransferService(LedgerDatabase database, TransferSteps steps, TransactionPolicy policy) {
        this.database = database;
        this.steps = steps;
        this.policy = policy;
    }

    @Override
    public TransferOutcome transfer(TransferCommand command) {
        for (var attempt = 0; ; attempt++) {
            try {
                return database.inTransaction(policy.isolation(), stores -> transferIn(stores, command));
            } catch (ConcurrencyConflict e) {
                if (attempt >= policy.retries()) {
                    throw e;
                }
            }
        }
    }

    private TransferOutcome transferIn(LedgerStores stores, TransferCommand command) {
        var hash = command.hash();
        var id = TransferId.random();
        if (!steps.claim().claim(stores.idempotency(), command.key(), hash, id)) {
            return replay(stores, command, hash);
        }
        var locked = steps.load().load(stores.accounts(), command.from(), command.to());
        var from = pick(locked, command.from());
        var to = pick(locked, command.to());
        return new Created(record(stores, id, command, TransferDecision.decide(from, to, command.amount())));
    }

    private static TransferOutcome replay(LedgerStores stores, TransferCommand command, RequestHash hash) {
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

    private Transfer record(LedgerStores stores, TransferId id, TransferCommand command, TransferDecision decision) {
        return switch (decision) {
            case Reject(var reason) ->
                stores.transfers().insert(newTransfer(id, command, TransferStatus.REJECTED, reason));
            case Post(var lines, var from, var to) -> {
                var transfer = stores.transfers().insert(newTransfer(id, command, TransferStatus.POSTED, null));
                stores.postings().insert(id, lines);
                Stream.of(from, to).sorted(Comparator.comparing(Account::id)).forEach(stores.accounts()::updateBalance);
                steps.publish().posted(stores, TransferPosted.of(UUID.randomUUID(), transfer, lines));
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
