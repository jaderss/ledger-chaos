package dev.jaderss.ledger.persistence;

import dev.jaderss.ledger.domain.IdempotencyKey;
import dev.jaderss.ledger.domain.RequestHash;
import dev.jaderss.ledger.domain.TransferId;
import java.util.Optional;
import java.util.UUID;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;

@Repository
public class IdempotencyStore {

    public record Entry(RequestHash hash, TransferId transferId) {}

    private final JdbcClient jdbc;

    public IdempotencyStore(JdbcClient jdbc) {
        this.jdbc = jdbc;
    }

    public boolean claim(IdempotencyKey key, RequestHash hash, TransferId transferId) {
        return jdbc.sql("""
                insert into idempotency (key, request_hash, transfer_id) values (:key, :hash, :transferId)
                on conflict (key) do nothing
                """)
                        .param("key", key.value())
                        .param("hash", hash.bytes())
                        .param("transferId", transferId.value())
                        .update()
                == 1;
    }

    public Optional<Entry> find(IdempotencyKey key) {
        return jdbc.sql("select request_hash, transfer_id from idempotency where key = :key")
                .param("key", key.value())
                .query((rs, row) -> new Entry(
                        RequestHash.fromBytes(rs.getBytes("request_hash")),
                        new TransferId(rs.getObject("transfer_id", UUID.class))))
                .optional();
    }
}
