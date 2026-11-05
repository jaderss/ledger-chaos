package dev.jaderss.ledger.persistence;

import dev.jaderss.ledger.domain.TransferPosted;
import org.jdbi.v3.core.Handle;
import tools.jackson.databind.json.JsonMapper;

public record OutboxStore(Handle handle, JsonMapper json) {

    public void append(TransferPosted event) {
        handle.createUpdate(
                        "insert into outbox (id, transfer_id, payload) values (:id, :transferId, cast(:payload as jsonb))")
                .bind("id", event.eventId())
                .bind("transferId", event.transferId())
                .bind("payload", json.writeValueAsString(event))
                .execute();
    }
}
