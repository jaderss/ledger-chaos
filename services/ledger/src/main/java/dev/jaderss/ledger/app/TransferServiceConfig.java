package dev.jaderss.ledger.app;

import dev.jaderss.ledger.app.LedgerProperties.Mode;
import dev.jaderss.ledger.app.TransferSteps.AccountLoad;
import dev.jaderss.ledger.app.TransferSteps.EventSink;
import dev.jaderss.ledger.app.TransferSteps.KeyClaim;
import dev.jaderss.ledger.messaging.DirectPublisher;
import dev.jaderss.ledger.persistence.LedgerStores;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.transaction.PlatformTransactionManager;
import tools.jackson.databind.json.JsonMapper;

@Configuration(proxyBeanMethods = false)
@EnableConfigurationProperties(LedgerProperties.class)
class TransferServiceConfig {

    @Bean
    TransferService transferService(
            LedgerProperties properties,
            PlatformTransactionManager transactionManager,
            LedgerStores stores,
            KafkaTemplate<String, String> kafka,
            JsonMapper json) {
        var direct = new DirectPublisher(kafka, json, properties.topic());
        return new LedgerTransferService(transactionManager, stores, steps(properties.mode(), stores, direct));
    }

    private static TransferSteps steps(Mode mode, LedgerStores stores, DirectPublisher direct) {
        KeyClaim claim = stores.idempotency()::claim;
        AccountLoad lockInIdOrder = stores.accounts()::lockInIdOrder;
        EventSink outbox = stores.outbox()::append;
        return switch (mode) {
            case CORRECT -> new TransferSteps(claim, lockInIdOrder, outbox);
            case NO_IDEMPOTENCY -> new TransferSteps((_, _, _) -> true, lockInIdOrder, outbox);
            case DUAL_WRITE -> new TransferSteps(claim, lockInIdOrder, direct::publishAfterCommit);
            case UNORDERED_LOCKS -> new TransferSteps(claim, stores.accounts()::lockInRequestOrder, outbox);
            case READ_MODIFY_WRITE -> new TransferSteps(claim, stores.accounts()::readWithoutLock, outbox);
        };
    }
}
