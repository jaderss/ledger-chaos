package dev.jaderss.ledger.domain;

import java.util.Currency;

public final class CurrencyMismatchException extends RuntimeException {

    public CurrencyMismatchException(Currency expected, Currency actual) {
        super("expected " + expected + " but got " + actual);
    }
}
