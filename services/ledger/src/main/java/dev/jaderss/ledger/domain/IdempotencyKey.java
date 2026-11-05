package dev.jaderss.ledger.domain;

import java.util.Objects;
import java.util.regex.Pattern;

public record IdempotencyKey(String value) {

    private static final Pattern VALID = Pattern.compile("[\\x21-\\x7E]{1,255}");

    public IdempotencyKey {
        Objects.requireNonNull(value, "value");
        if (!VALID.matcher(value).matches()) {
            throw new IllegalArgumentException("idempotency key must be 1 to 255 visible ASCII characters");
        }
    }

    @Override
    public String toString() {
        return value;
    }
}
