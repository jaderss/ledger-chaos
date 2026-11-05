package dev.jaderss.ledger.app;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import dev.jaderss.ledger.Fixtures;
import dev.jaderss.ledger.PostgresContainer;
import java.time.Duration;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;

@SpringBootTest(properties = "ledger.mode=unordered-locks")
@Import({PostgresContainer.class, Fixtures.class})
class UnorderedLocksModeTest {

    @Autowired
    Fixtures fixtures;

    @Test
    @DisplayName("a transfer that locks from before to deadlocks with a transaction that locks in id order")
    void deadlocks() {
        var ids = fixtures.inIdOrder(fixtures.account(1_000), fixtures.account(1_000));
        var low = ids.getFirst();
        var high = ids.getLast();
        var deadlocksBefore = fixtures.deadlocks();

        fixtures.race(() -> fixtures.lock(low), () -> fixtures.lock(high), () -> fixtures.transfer(high, low, 1));

        await().atMost(Duration.ofSeconds(10))
                .untilAsserted(() -> assertThat(fixtures.deadlocks()).isGreaterThan(deadlocksBefore));
    }
}
