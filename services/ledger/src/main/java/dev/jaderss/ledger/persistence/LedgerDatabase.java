package dev.jaderss.ledger.persistence;

import java.util.function.Function;
import javax.sql.DataSource;
import org.jdbi.v3.core.Jdbi;
import org.jdbi.v3.core.JdbiException;
import org.jdbi.v3.postgres.PostgresPlugin;
import org.springframework.boot.sql.init.dependency.DependsOnDatabaseInitialization;
import org.springframework.stereotype.Component;
import tools.jackson.databind.json.JsonMapper;

@Component
@DependsOnDatabaseInitialization
public class LedgerDatabase {

    private final Jdbi jdbi;
    private final JsonMapper json;

    public LedgerDatabase(DataSource dataSource, JsonMapper json) {
        this.jdbi = Jdbi.create(dataSource).installPlugin(new PostgresPlugin());
        this.json = json;
    }

    public <T> T inTransaction(Isolation isolation, Function<LedgerStores, T> work) {
        try {
            return jdbi.inTransaction(handle -> {
                isolation.firstStatement().ifPresent(handle::execute);
                return work.apply(LedgerStores.on(handle, json));
            });
        } catch (JdbiException e) {
            throw DatabaseException.from(e);
        }
    }

    public <T> T withStores(Function<LedgerStores, T> work) {
        try {
            return jdbi.withHandle(handle -> work.apply(LedgerStores.on(handle, json)));
        } catch (JdbiException e) {
            throw DatabaseException.from(e);
        }
    }
}
