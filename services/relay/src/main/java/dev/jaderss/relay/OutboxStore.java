package dev.jaderss.relay;

import java.util.List;
import java.util.UUID;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;

@Repository
class OutboxStore {

    record Row(UUID id, UUID transferId, String correlationId, String payload) {}

    private final JdbcClient jdbc;

    OutboxStore(JdbcClient jdbc) {
        this.jdbc = jdbc;
    }

    List<Row> claim(int limit) {
        return jdbc.sql("""
                select id, transfer_id, payload ->> 'correlationId' as correlation_id, payload::text as payload
                  from outbox
                 where published_at is null
                 order by seq
                 limit :limit
                   for update skip locked
                """)
                .param("limit", limit)
                .query((rs, n) -> new Row(
                        rs.getObject("id", UUID.class),
                        rs.getObject("transfer_id", UUID.class),
                        rs.getString("correlation_id"),
                        rs.getString("payload")))
                .list();
    }

    void markPublished(List<UUID> ids) {
        jdbc.sql("update outbox set published_at = now() where id = any(:ids)")
                .param("ids", ids.toArray(UUID[]::new))
                .update();
    }
}
