package dev.jaderss.ledger.app;

import dev.jaderss.ledger.domain.Account;
import dev.jaderss.ledger.domain.AccountId;
import dev.jaderss.ledger.domain.IdempotencyKey;
import dev.jaderss.ledger.domain.RequestHash;
import dev.jaderss.ledger.domain.TransferId;
import dev.jaderss.ledger.domain.TransferPosted;
import java.util.List;
import java.util.Objects;

record TransferSteps(KeyClaim claim, AccountLoad load, EventSink publish) {

    TransferSteps {
        Objects.requireNonNull(claim, "claim");
        Objects.requireNonNull(load, "load");
        Objects.requireNonNull(publish, "publish");
    }

    @FunctionalInterface
    interface KeyClaim {
        boolean claim(IdempotencyKey key, RequestHash hash, TransferId transferId);
    }

    @FunctionalInterface
    interface AccountLoad {
        List<Account> load(AccountId from, AccountId to);
    }

    @FunctionalInterface
    interface EventSink {
        void posted(TransferPosted event);
    }
}
