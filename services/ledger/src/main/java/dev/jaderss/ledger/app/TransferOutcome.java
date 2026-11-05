package dev.jaderss.ledger.app;

import dev.jaderss.ledger.domain.IdempotencyKey;
import dev.jaderss.ledger.domain.Transfer;

public sealed interface TransferOutcome {

    record Created(Transfer transfer) implements TransferOutcome {}

    record Replayed(Transfer transfer) implements TransferOutcome {}

    record KeyMismatch(IdempotencyKey key) implements TransferOutcome {}
}
