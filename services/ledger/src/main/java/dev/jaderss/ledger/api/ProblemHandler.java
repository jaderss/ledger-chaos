package dev.jaderss.ledger.api;

import dev.jaderss.ledger.domain.AccountNotFoundException;
import dev.jaderss.ledger.domain.CurrencyMismatchException;
import dev.jaderss.ledger.persistence.DatabaseException;
import dev.jaderss.ledger.persistence.DatabaseException.IntegrityViolation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ProblemDetail;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.web.servlet.mvc.method.annotation.ResponseEntityExceptionHandler;

@RestControllerAdvice
class ProblemHandler extends ResponseEntityExceptionHandler {

    private static final Logger log = LoggerFactory.getLogger(ProblemHandler.class);

    @ExceptionHandler
    ProblemDetail accountNotFound(AccountNotFoundException e) {
        return Problems.of(HttpStatus.NOT_FOUND, "account-not-found", e.getMessage());
    }

    @ExceptionHandler
    ProblemDetail currencyMismatch(CurrencyMismatchException e) {
        return Problems.of(HttpStatus.UNPROCESSABLE_CONTENT, "currency-mismatch", e.getMessage());
    }

    @ExceptionHandler
    ProblemDetail keyRequired(IdempotencyKeyRequiredException e) {
        return Problems.of(HttpStatus.BAD_REQUEST, "idempotency-key-required", e.getMessage());
    }

    @ExceptionHandler
    ProblemDetail invalid(InvalidRequestException e) {
        return Problems.of(HttpStatus.BAD_REQUEST, "invalid-request", e.getMessage());
    }

    @ExceptionHandler
    ProblemDetail overflow(ArithmeticException e) {
        log.error("amount overflow", e);
        return Problems.of(HttpStatus.INTERNAL_SERVER_ERROR, "ledger-integrity", "amount out of range");
    }

    @ExceptionHandler
    ProblemDetail integrity(IntegrityViolation e) {
        log.error("ledger integrity check failed", e);
        return Problems.of(HttpStatus.INTERNAL_SERVER_ERROR, "ledger-integrity", "the ledger refused the write");
    }

    @ExceptionHandler
    ProblemDetail unavailable(DatabaseException e) {
        log.warn("database unavailable: {}", e.getMessage());
        return Problems.of(HttpStatus.SERVICE_UNAVAILABLE, "unavailable", "outcome unknown, retry with the same key");
    }
}
