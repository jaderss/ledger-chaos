package dev.jaderss.ledger.persistence;

import java.sql.SQLException;
import java.util.stream.Stream;

public abstract sealed class DatabaseException extends RuntimeException {

    private static final String INTEGRITY_CONSTRAINT_VIOLATION = "23";
    private static final String TRANSACTION_ROLLBACK = "40";

    private DatabaseException(String message, Throwable cause) {
        super(message, cause);
    }

    public static final class ConcurrencyConflict extends DatabaseException {
        private ConcurrencyConflict(String message, Throwable cause) {
            super(message, cause);
        }
    }

    public static final class IntegrityViolation extends DatabaseException {
        private IntegrityViolation(String message, Throwable cause) {
            super(message, cause);
        }
    }

    public static final class Unavailable extends DatabaseException {
        private Unavailable(String message, Throwable cause) {
            super(message, cause);
        }
    }

    static DatabaseException from(RuntimeException failure) {
        var sql = Stream.<Throwable>iterate(failure, Throwable::getCause)
                .takeWhile(cause -> cause != null)
                .filter(SQLException.class::isInstance)
                .map(SQLException.class::cast)
                .findFirst();
        var message = sql.map(Throwable::getMessage).orElse(failure.getMessage());
        var sqlClass = sql.map(SQLException::getSQLState)
                .filter(state -> state.length() >= 2)
                .map(state -> state.substring(0, 2))
                .orElse("");
        return switch (sqlClass) {
            case INTEGRITY_CONSTRAINT_VIOLATION -> new IntegrityViolation(message, failure);
            case TRANSACTION_ROLLBACK -> new ConcurrencyConflict(message, failure);
            default -> new Unavailable(message, failure);
        };
    }
}
