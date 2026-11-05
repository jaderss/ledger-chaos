package dev.jaderss.checker;

import static java.util.stream.Collectors.toMap;

import dev.jaderss.checker.Config.Postgres;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringSerializer;
import org.jdbi.v3.core.Jdbi;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.postgresql.PostgreSQLContainer;

final class Containers {

    private static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:18.6");
    private static final KafkaContainer KAFKA = new KafkaContainer("apache/kafka:4.4.0");

    private Containers() {}

    static synchronized Postgres database(String schema) {
        if (!POSTGRES.isRunning()) {
            POSTGRES.start();
        }
        var name = "checker_" + UUID.randomUUID().toString().replace("-", "");
        Jdbi.create(POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword())
                .useHandle(h -> h.execute("create database " + name));
        var url = POSTGRES.getJdbcUrl().replaceFirst("/[^/?]+(\\?|$)", "/" + name + "$1");
        var database = new Postgres(url, POSTGRES.getUsername(), POSTGRES.getPassword());
        Jdbi.create(url, database.user(), database.password())
                .useHandle(h -> h.createScript(schema).execute());
        return database;
    }

    static String dsn(Postgres database) {
        var path = database.url().substring("jdbc:postgresql://".length());
        return "postgres://%s:%s@%s".formatted(database.user(), database.password(), path);
    }

    static synchronized String brokers() {
        if (!KAFKA.isRunning()) {
            KAFKA.start();
        }
        return KAFKA.getBootstrapServers();
    }

    static Admin admin() {
        var props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, brokers());
        return Admin.create(props);
    }

    static String topic(Admin admin, int partitions) throws Exception {
        var topic = "checker-test-" + UUID.randomUUID();
        admin.createTopics(List.of(new NewTopic(topic, partitions, (short) 1)))
                .all()
                .get();
        return topic;
    }

    static void commitToEnd(Admin admin, String group, String topic) throws Exception {
        var partitions = admin.describeTopics(List.of(topic)).allTopicNames().get().get(topic).partitions().stream()
                .collect(toMap(p -> new TopicPartition(topic, p.partition()), _ -> OffsetSpec.latest()));
        var ends = admin.listOffsets(partitions).all().get();
        var offsets = ends.entrySet().stream()
                .collect(toMap(
                        Map.Entry::getKey,
                        e -> new OffsetAndMetadata(e.getValue().offset())));
        admin.alterConsumerGroupOffsets(group, offsets).all().get();
    }

    static void produce(String topic, String... values) throws Exception {
        var props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers());
        try (var producer = new KafkaProducer<>(props, new StringSerializer(), new StringSerializer())) {
            for (var i = 0; i < values.length; i++) {
                producer.send(new ProducerRecord<>(topic, String.valueOf(i), values[i]))
                        .get();
            }
        }
    }
}
