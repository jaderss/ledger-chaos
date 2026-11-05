package dev.jaderss.ledger.persistence;

import static org.assertj.core.api.Assertions.assertThat;

import dev.jaderss.ledger.PostgresContainer;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;

@SpringBootTest
@Import(PostgresContainer.class)
class IsolationTest {

    @Autowired
    LedgerDatabase database;

    @ParameterizedTest
    @CsvSource({"READ_COMMITTED, read committed", "SERIALIZABLE, serializable"})
    @DisplayName("each transaction runs at the isolation level it asks for")
    void transactionRunsAtItsLevel(Isolation isolation, String postgresName) {
        var level = database.inTransaction(
                isolation,
                stores -> stores.handle()
                        .createQuery("show transaction_isolation")
                        .mapTo(String.class)
                        .one());

        assertThat(level).isEqualTo(postgresName);
    }
}
