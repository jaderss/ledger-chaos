package dev.jaderss.ledger.app;

import dev.jaderss.ledger.persistence.AccountStore;
import dev.jaderss.ledger.persistence.IdempotencyStore;
import dev.jaderss.ledger.persistence.OutboxStore;
import dev.jaderss.ledger.persistence.PostingStore;
import dev.jaderss.ledger.persistence.TransferStore;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.transaction.PlatformTransactionManager;

@Configuration
@EnableConfigurationProperties(LedgerProperties.class)
class TransferServiceConfig {

    @Bean
    TransferService transferService(
            LedgerProperties properties,
            PlatformTransactionManager transactionManager,
            IdempotencyStore idempotency,
            AccountStore accounts,
            TransferStore transfers,
            PostingStore postings,
            OutboxStore outbox) {
        return switch (properties.mode()) {
            case CORRECT ->
                switch (properties.locking()) {
                    case ORDERED ->
                        new LockingTransferService(
                                transactionManager, idempotency, accounts, transfers, postings, outbox);
                };
        };
    }
}
