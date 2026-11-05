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
import org.jdbi.v3.core.Handle;
import org.jdbi.v3.core.statement.StatementContext;

public record TransferStore(Handle handle) {

    private static final String COLUMNS = """
            select id, request_key, correlation_id, from_account, to_account, amount_minor, currency,
                   status, reject_reason, created_at
              from transfer
            """;

    public Transfer insert(Transfer transfer) {
        return handle.createQuery("""
                        insert into transfer (id, request_key, correlation_id, from_account, to_account,
                                              amount_minor, currency, status, reject_reason)
                        values (:id, :key, :correlationId, :from, :to, :amount, :currency, :status, :reason)
                        returning id, request_key, correlation_id, from_account, to_account, amount_minor, currency,
                                  status, reject_reason, created_at
                        """)
                .bind("id", transfer.id().value())
                .bind("key", transfer.key().value())
                .bind("correlationId", transfer.correlationId())
                .bind("from", transfer.from().value())
                .bind("to", transfer.to().value())
                .bind("amount", transfer.amount().amountMinor())
                .bind("currency", transfer.amount().currency().getCurrencyCode())
                .bind("status", transfer.status().name())
                .bind("reason", transfer.reason().map(Enum::name).orElse(null))
                .map(TransferStore::map)
                .one();
    }

    public Optional<Transfer> find(TransferId id) {
        return handle.createQuery(COLUMNS + "where id = :id")
                .bind("id", id.value())
                .map(TransferStore::map)
                .findOne();
    }

    public List<Transfer> findByKey(IdempotencyKey key) {
        return handle.createQuery(COLUMNS + "where request_key = :key order by created_at")
                .bind("key", key.value())
                .map(TransferStore::map)
                .list();
    }

    private static Transfer map(ResultSet rs, StatementContext context) throws SQLException {
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
