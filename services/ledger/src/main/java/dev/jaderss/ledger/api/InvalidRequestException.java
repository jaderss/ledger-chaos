package dev.jaderss.ledger.api;

import java.util.function.Supplier;

final class InvalidRequestException extends RuntimeException {

    private InvalidRequestException(IllegalArgumentException cause) {
        super(cause.getMessage(), cause);
    }

    static <T> T parse(Supplier<T> parser) {
        try {
            return parser.get();
        } catch (IllegalArgumentException e) {
            throw new InvalidRequestException(e);
        }
    }
}
