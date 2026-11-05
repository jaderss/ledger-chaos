package dev.jaderss.ledger.app;

import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import dev.jaderss.ledger.app.LedgerProperties.Locking;
import dev.jaderss.ledger.app.LedgerProperties.Mode;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class LedgerPropertiesTest {

    @Test
    @DisplayName("serializable locking with a naive mode refuses to start")
    void serializableOnlyWithCorrect() {
        assertThatThrownBy(() -> new LedgerProperties(Mode.READ_MODIFY_WRITE, Locking.SERIALIZABLE, 5, "t"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("READ_MODIFY_WRITE");
        assertThatNoException().isThrownBy(() -> new LedgerProperties(Mode.CORRECT, Locking.SERIALIZABLE, 5, "t"));
        assertThatNoException().isThrownBy(() -> new LedgerProperties(Mode.READ_MODIFY_WRITE, Locking.ORDERED, 5, "t"));
    }
}
