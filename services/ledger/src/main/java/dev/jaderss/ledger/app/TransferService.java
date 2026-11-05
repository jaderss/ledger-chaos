package dev.jaderss.ledger.app;

import dev.jaderss.ledger.domain.TransferCommand;

public interface TransferService {

    TransferOutcome transfer(TransferCommand command);
}
