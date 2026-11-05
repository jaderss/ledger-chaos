package dev.jaderss.ledger.domain;

public final class AccountNotFoundException extends RuntimeException {

    public AccountNotFoundException(AccountId id) {
        super("account " + id + " not found");
    }
}
