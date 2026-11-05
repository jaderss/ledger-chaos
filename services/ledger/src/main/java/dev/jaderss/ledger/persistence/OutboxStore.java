package dev.jaderss.ledger.persistence;

import dev.jaderss.ledger.domain.TransferPosted;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;
import tools.jackson.databind.json.JsonMapper;

@Repository
public class OutboxStore {

    private final JdbcClient jdbc;
    private final JsonMapper json;

    public OutboxStore(JdbcClient jdbc, JsonMapper json) {
        this.jdbc = jdbc;
        this.json = json;
    }

    public void append(TransferPosted event) {
        jdbc.sql("insert into outbox (id, transfer_id, payload) values (:id, :transferId, cast(:payload as jsonb))")
                .param("id", event.eventId())
                .param("transferId", event.transferId())
                .param("payload", json.writeValueAsString(event))
                .update();
    }
}
