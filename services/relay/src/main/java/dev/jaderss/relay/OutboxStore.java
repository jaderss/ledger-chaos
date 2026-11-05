package dev.jaderss.relay;

import java.util.List;
import java.util.UUID;
import org.jdbi.v3.core.Handle;

record OutboxStore(Handle handle) {

    record Row(UUID id, UUID transferId, String correlationId, String payload) {}

    List<Row> claim(int limit) {
        return handle.createQuery("""
                        select id, transfer_id, payload ->> 'correlationId' as correlation_id, payload::text as payload
                          from outbox
                         where published_at is null
                         order by seq
                         limit :limit
                           for update skip locked
                        """)
                .bind("limit", limit)
                .map((rs, _) -> new Row(
                        rs.getObject("id", UUID.class),
                        rs.getObject("transfer_id", UUID.class),
                        rs.getString("correlation_id"),
                        rs.getString("payload")))
                .list();
    }

    void markPublished(List<UUID> ids) {
        handle.createUpdate("update outbox set published_at = now() where id = any(:ids)")
                .bindArray("ids", UUID.class, ids)
                .execute();
    }
}
