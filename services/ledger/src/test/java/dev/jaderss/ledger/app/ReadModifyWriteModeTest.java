package dev.jaderss.ledger.app;

import static org.assertj.core.api.Assertions.assertThat;

import dev.jaderss.ledger.Fixtures;
import dev.jaderss.ledger.PostgresContainer;
import dev.jaderss.ledger.app.TransferOutcome.Created;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;

@SpringBootTest(properties = "ledger.mode=read-modify-write")
@Import({PostgresContainer.class, Fixtures.class})
class ReadModifyWriteModeTest {

    @Autowired
    Fixtures fixtures;

    @Test
    @DisplayName("a transfer that read the balance before another one committed overwrites it")
    void lostUpdate() throws Exception {
        var from = fixtures.account(1_000);
        var to = fixtures.account(0);
        var other = fixtures.account(1_000);

        var racer =
                fixtures.race(() -> fixtures.transfer(other, to, 5), () -> {}, () -> fixtures.transfer(from, to, 1));

        assertThat(racer.get()).isInstanceOf(Created.class);
        assertThat(fixtures.sumOfPostings(to)).isEqualTo(6);
        assertThat(fixtures.balance(to)).isEqualTo(1);
    }
}
