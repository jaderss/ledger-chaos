package dev.jaderss.ledger.api;

import dev.jaderss.ledger.app.TransferOutcome.Created;
import dev.jaderss.ledger.app.TransferOutcome.KeyMismatch;
import dev.jaderss.ledger.app.TransferOutcome.Replayed;
import dev.jaderss.ledger.app.TransferService;
import dev.jaderss.ledger.domain.IdempotencyKey;
import dev.jaderss.ledger.domain.Transfer;
import dev.jaderss.ledger.domain.TransferId;
import dev.jaderss.ledger.domain.TransferStatus;
import dev.jaderss.ledger.persistence.LedgerDatabase;
import jakarta.validation.Valid;
import java.net.URI;
import java.util.List;
import java.util.Locale;
import java.util.UUID;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestAttribute;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/transfers")
class TransferController {

    static final String REPLAYED = "Idempotent-Replayed";

    private final TransferService service;
    private final LedgerDatabase database;

    TransferController(TransferService service, LedgerDatabase database) {
        this.service = service;
        this.database = database;
    }

    @PostMapping
    ResponseEntity<Object> create(
            @RequestHeader(name = "Idempotency-Key", required = false) String key,
            @RequestAttribute(CorrelationIdFilter.ATTRIBUTE) String correlationId,
            @Valid @RequestBody TransferRequest request) {
        return switch (service.transfer(
                InvalidRequestException.parse(() -> request.toCommand(idempotencyKey(key), correlationId)))) {
            case Created(var t)
            when t.status() == TransferStatus.POSTED ->
                ResponseEntity.created(URI.create("/transfers/" + t.id())).body(TransferResponse.from(t));
            case Created(var t) -> rejected(ResponseEntity.unprocessableContent(), t);
            case Replayed(var t)
            when t.status() == TransferStatus.POSTED ->
                ResponseEntity.ok().header(REPLAYED, "true").body(TransferResponse.from(t));
            case Replayed(var t) ->
                rejected(ResponseEntity.unprocessableContent().header(REPLAYED, "true"), t);
            case KeyMismatch(var k) ->
                ResponseEntity.unprocessableContent()
                        .body(Problems.of(
                                HttpStatus.UNPROCESSABLE_CONTENT,
                                "idempotency-key-mismatch",
                                "key " + k + " was used with another request"));
        };
    }

    @GetMapping("/{id}")
    ResponseEntity<TransferResponse> get(@PathVariable UUID id) {
        return ResponseEntity.of(
                database.withStores(stores -> stores.transfers().find(new TransferId(id)))
                        .map(TransferResponse::from));
    }

    @GetMapping
    List<TransferResponse> byKey(@RequestParam String idempotencyKey) {
        var key = InvalidRequestException.parse(() -> new IdempotencyKey(idempotencyKey));
        return database.withStores(stores -> stores.transfers().findByKey(key)).stream()
                .map(TransferResponse::from)
                .toList();
    }

    private static IdempotencyKey idempotencyKey(String key) {
        try {
            return new IdempotencyKey(key == null ? "" : key);
        } catch (IllegalArgumentException e) {
            throw new IdempotencyKeyRequiredException();
        }
    }

    private static ResponseEntity<Object> rejected(ResponseEntity.BodyBuilder response, Transfer transfer) {
        var problem = Problems.of(
                HttpStatus.UNPROCESSABLE_CONTENT,
                transfer.reason().orElseThrow().name().toLowerCase(Locale.ROOT).replace('_', '-'),
                "transfer " + transfer.id() + " was rejected");
        problem.setProperty("transferId", transfer.id().value());
        problem.setProperty("transferStatus", transfer.status().name());
        return response.body(problem);
    }
}
