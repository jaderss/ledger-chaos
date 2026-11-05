package dev.jaderss.ledger.persistence;

import dev.jaderss.ledger.domain.AccountId;
import dev.jaderss.ledger.domain.IdempotencyKey;
import dev.jaderss.ledger.domain.Money;
import dev.jaderss.ledger.domain.RejectReason;
import dev.jaderss.ledger.domain.Transfer;
import dev.jaderss.ledger.domain.TransferId;
import dev.jaderss.ledger.domain.TransferStatus;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;

@Repository
public class TransferStore {

    private static final String COLUMNS = """
            select id, request_key, correlation_id, from_account, to_account, amount_minor, currency,
                   status, reject_reason, created_at
              from transfer
            """;

    private final JdbcClient jdbc;

    public TransferStore(JdbcClient jdbc) {
        this.jdbc = jdbc;
    }

    public Transfer insert(Transfer transfer) {
        return jdbc.sql("""
                insert into transfer (id, request_key, correlation_id, from_account, to_account,
                                      amount_minor, currency, status, reject_reason)
                values (:id, :key, :correlationId, :from, :to, :amount, :currency, :status, :reason)
                returning id, request_key, correlation_id, from_account, to_account, amount_minor, currency,
                          status, reject_reason, created_at
                """)
                .param("id", transfer.id().value())
                .param("key", transfer.key().value())
                .param("correlationId", transfer.correlationId())
                .param("from", transfer.from().value())
                .param("to", transfer.to().value())
                .param("amount", transfer.amount().amountMinor())
                .param("currency", transfer.amount().currency().getCurrencyCode())
                .param("status", transfer.status().name())
                .param("reason", transfer.reason().map(Enum::name).orElse(null))
                .query(TransferStore::map)
                .single();
    }

    public Optional<Transfer> find(TransferId id) {
        return jdbc.sql(COLUMNS + "where id = :id")
                .param("id", id.value())
                .query(TransferStore::map)
                .optional();
    }

    public List<Transfer> findByKey(IdempotencyKey key) {
        return jdbc.sql(COLUMNS + "where request_key = :key order by created_at")
                .param("key", key.value())
                .query(TransferStore::map)
                .list();
    }

    private static Transfer map(ResultSet rs, int row) throws SQLException {
        var reason = rs.getString("reject_reason");
        return new Transfer(
                new TransferId(rs.getObject("id", UUID.class)),
                new IdempotencyKey(rs.getString("request_key")),
                rs.getString("correlation_id"),
                new AccountId(rs.getObject("from_account", UUID.class)),
                new AccountId(rs.getObject("to_account", UUID.class)),
                Money.of(rs.getLong("amount_minor"), rs.getString("currency")),
                TransferStatus.valueOf(rs.getString("status")),
                reason == null ? null : RejectReason.valueOf(reason),
                rs.getObject("created_at", OffsetDateTime.class).toInstant());
    }
}
