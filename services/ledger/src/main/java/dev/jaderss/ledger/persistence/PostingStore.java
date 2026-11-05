package dev.jaderss.ledger.persistence;

import dev.jaderss.ledger.domain.Posting;
import dev.jaderss.ledger.domain.TransferId;
import java.util.List;
import org.jdbi.v3.core.Handle;

public record PostingStore(Handle handle) {

    public void insert(TransferId transferId, List<Posting> postings) {
        postings.forEach(posting -> handle.createUpdate(
                        "insert into posting (transfer_id, account_id, amount_minor) values (:transferId, :account, :amount)")
                .bind("transferId", transferId.value())
                .bind("account", posting.accountId().value())
                .bind("amount", posting.amount().amountMinor())
                .execute());
    }
}
