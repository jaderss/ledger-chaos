package dev.jaderss.ledger.persistence;

import dev.jaderss.ledger.domain.IdempotencyKey;
import dev.jaderss.ledger.domain.RequestHash;
import dev.jaderss.ledger.domain.TransferId;
import java.util.Optional;
import java.util.UUID;
import org.jdbi.v3.core.Handle;

public record IdempotencyStore(Handle handle) {

    public record Entry(RequestHash hash, TransferId transferId) {}

    public boolean claim(IdempotencyKey key, RequestHash hash, TransferId transferId) {
        return handle.createUpdate("""
                        insert into idempotency (key, request_hash, transfer_id) values (:key, :hash, :transferId)
                        on conflict (key) do nothing
                        """)
                        .bind("key", key.value())
                        .bind("hash", hash.bytes())
                        .bind("transferId", transferId.value())
                        .execute()
                == 1;
    }

    public Optional<Entry> find(IdempotencyKey key) {
        return handle.createQuery("select request_hash, transfer_id from idempotency where key = :key")
                .bind("key", key.value())
                .map((rs, _) -> new Entry(
                        RequestHash.fromBytes(rs.getBytes("request_hash")),
                        new TransferId(rs.getObject("transfer_id", UUID.class))))
                .findOne();
    }
}
