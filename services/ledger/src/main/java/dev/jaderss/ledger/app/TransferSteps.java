package dev.jaderss.ledger.app;

import dev.jaderss.ledger.domain.Account;
import dev.jaderss.ledger.domain.AccountId;
import dev.jaderss.ledger.domain.IdempotencyKey;
import dev.jaderss.ledger.domain.RequestHash;
import dev.jaderss.ledger.domain.TransferId;
import dev.jaderss.ledger.domain.TransferPosted;
import dev.jaderss.ledger.persistence.AccountStore;
import dev.jaderss.ledger.persistence.IdempotencyStore;
import dev.jaderss.ledger.persistence.LedgerStores;
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
        boolean claim(IdempotencyStore store, IdempotencyKey key, RequestHash hash, TransferId transferId);
    }

    @FunctionalInterface
    interface AccountLoad {
        List<Account> load(AccountStore store, AccountId from, AccountId to);
    }

    @FunctionalInterface
    interface EventSink {
        void posted(LedgerStores stores, TransferPosted event);
    }
}
