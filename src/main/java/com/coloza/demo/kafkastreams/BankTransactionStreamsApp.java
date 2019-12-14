package com.coloza.demo.kafkastreams;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.connect.json.JsonDeserializer;
import org.apache.kafka.connect.json.JsonSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;

import java.time.Instant;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class BankTransactionStreamsApp {

    private static final String DEFAULT_BOOTSTRAP_SERVER = "localhost:9092";
    private static final String APPLICATION_ID = "bank-transaction-streams-app";
    private static final String QUERYABLE_STORE_NAME = "bank-transaction-aggregate";
    private static final String SHUTDOWN_HOOK_THREAD_NAME = "bank-transaction-streams-shutdown-hook";
    private String bootstrapServer;

    public BankTransactionStreamsApp() {
        this(DEFAULT_BOOTSTRAP_SERVER);
    }

    public BankTransactionStreamsApp(String bootstrapServer) {
        this.bootstrapServer = bootstrapServer;
    }

    private Properties createProperties() {
        Properties properties = new Properties();
        properties.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, this.APPLICATION_ID);
        properties.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, this.bootstrapServer);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, OffsetResetStrategy.EARLIEST.name().toLowerCase());
        properties.setProperty(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE);
        return properties;
    }

    public Topology createTopology(String fromTopic, String toTopic) {
        Serde<JsonNode> jsonSerde = Serdes.serdeFrom(new JsonSerializer(), new JsonDeserializer());

        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, JsonNode> bankTransactionInput = builder.stream(fromTopic, Consumed.with(Serdes.String(), jsonSerde));

        ObjectNode initialBalance = JsonNodeFactory.instance.objectNode();
        initialBalance.put("count", 0);
        initialBalance.put("balance", 0);
        initialBalance.put("time", Instant.ofEpochMilli(0L).toString());

        KTable<String, JsonNode> bankBalance = bankTransactionInput
                .groupByKey(Grouped.with(Serdes.String(), jsonSerde))
                .aggregate(
                        () -> initialBalance,
                        (key, transaction, balance) -> newBalance(transaction, balance),
                        Materialized.<String, JsonNode, KeyValueStore<Bytes, byte[]>>as(QUERYABLE_STORE_NAME)
                            .withKeySerde(Serdes.String())
                            .withValueSerde(jsonSerde)
                );
        bankBalance.toStream().to(toTopic, Produced.with(Serdes.String(), jsonSerde));
        return builder.build();
    }

    private JsonNode newBalance(JsonNode transaction, JsonNode balance) {
        ObjectNode newBalance = JsonNodeFactory.instance.objectNode();
        newBalance.put("count", balance.get("count").asInt() + 1);
        newBalance.put("balance", balance.get("balance").asInt() + transaction.get("amount").asInt());

        Long balanceEpoch = Instant.parse(balance.get("time").asText()).toEpochMilli();
        Long transactionEpoch = Instant.parse(transaction.get("time").asText()).toEpochMilli();
        Instant newBalanceInstant = Instant.ofEpochMilli(Math.max(balanceEpoch, transactionEpoch));
        newBalance.put("time", newBalanceInstant.toString());
        return newBalance;
    }

    public static void main(String[] args) {
        String inputTopic = "streams-bank-transaction-input";
        String outputTopic = "streams-bank-transaction-output";
        if (args.length > 2) {
            outputTopic = args[2];
        }
        if (args.length > 1) {
            inputTopic = args[1];
        }
        String bootstrapServer = DEFAULT_BOOTSTRAP_SERVER;
        if (args.length > 0) {
            bootstrapServer = args[0];
        }
        BankTransactionStreamsApp app = new BankTransactionStreamsApp(bootstrapServer);
        // build the topology and start our streams
        KafkaStreams streams = new KafkaStreams(app.createTopology(inputTopic, outputTopic), app.createProperties());
        CountDownLatch latch = new CountDownLatch(1);

        // shutdown hook to correctly close the streams application
        // attach shutdown handler to catch control Ctrl+C
        Runtime.getRuntime().addShutdownHook(new Thread(SHUTDOWN_HOOK_THREAD_NAME) {
            @Override
            public void run() {
                streams.close();
                latch.countDown();
            }
        });

        try {
            streams.start();
            // printed the topology
            System.out.println(streams.toString());
            latch.await();
        } catch (Throwable e) {
            System.exit(1);
        }
    }
}
