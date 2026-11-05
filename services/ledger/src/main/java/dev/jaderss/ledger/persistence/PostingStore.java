package dev.jaderss.ledger.persistence;

import dev.jaderss.ledger.domain.Posting;
import dev.jaderss.ledger.domain.TransferId;
import java.util.List;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;

@Repository
public class PostingStore {

    private final JdbcClient jdbc;

    public PostingStore(JdbcClient jdbc) {
        this.jdbc = jdbc;
    }

    public void insert(TransferId transferId, List<Posting> postings) {
        postings.forEach(posting -> jdbc.sql(
                        "insert into posting (transfer_id, account_id, amount_minor) values (:transferId, :account, :amount)")
                .param("transferId", transferId.value())
                .param("account", posting.accountId().value())
                .param("amount", posting.amount().amountMinor())
                .update());
    }
}
