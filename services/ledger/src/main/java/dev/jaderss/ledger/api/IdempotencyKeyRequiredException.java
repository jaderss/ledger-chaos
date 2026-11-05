package dev.jaderss.ledger.api;

final class IdempotencyKeyRequiredException extends RuntimeException {

    IdempotencyKeyRequiredException() {
        super("header Idempotency-Key is required, 1 to 255 visible ASCII characters");
    }
}
