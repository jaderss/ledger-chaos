package dev.jaderss.ledger.app;

import dev.jaderss.ledger.app.LedgerProperties.Mode;
import dev.jaderss.ledger.app.TransferSteps.AccountLoad;
import dev.jaderss.ledger.app.TransferSteps.EventSink;
import dev.jaderss.ledger.app.TransferSteps.KeyClaim;
import dev.jaderss.ledger.messaging.DirectPublisher;
import dev.jaderss.ledger.persistence.AccountStore;
import dev.jaderss.ledger.persistence.IdempotencyStore;
import dev.jaderss.ledger.persistence.LedgerDatabase;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.KafkaTemplate;
import tools.jackson.databind.json.JsonMapper;

@Configuration(proxyBeanMethods = false)
@EnableConfigurationProperties(LedgerProperties.class)
class TransferServiceConfig {

    @Bean
    TransferService transferService(
            LedgerProperties properties,
            LedgerDatabase database,
            KafkaTemplate<String, String> kafka,
            JsonMapper json) {
        var direct = new DirectPublisher(kafka, json, properties.topic());
        var steps = steps(properties.mode(), direct);
        return switch (properties.locking()) {
            case ORDERED -> new LedgerTransferService(database, steps, TransactionPolicy.readCommitted());
            case SERIALIZABLE ->
                new LedgerTransferService(
                        database,
                        new TransferSteps(steps.claim(), AccountStore::readWithoutLock, steps.publish()),
                        TransactionPolicy.serializable(properties.serializableRetries()));
        };
    }

    private static TransferSteps steps(Mode mode, DirectPublisher direct) {
        KeyClaim claim = IdempotencyStore::claim;
        AccountLoad lockInIdOrder = AccountStore::lockInIdOrder;
        EventSink outbox = (stores, event) -> stores.outbox().append(event);
        EventSink afterCommit = (stores, event) -> stores.afterCommit(() -> direct.send(event));
        return switch (mode) {
            case CORRECT -> new TransferSteps(claim, lockInIdOrder, outbox);
            case NO_IDEMPOTENCY -> new TransferSteps((_, _, _, _) -> true, lockInIdOrder, outbox);
            case DUAL_WRITE -> new TransferSteps(claim, lockInIdOrder, afterCommit);
            case UNORDERED_LOCKS -> new TransferSteps(claim, AccountStore::lockInRequestOrder, outbox);
            case READ_MODIFY_WRITE -> new TransferSteps(claim, AccountStore::readWithoutLock, outbox);
        };
    }
}
