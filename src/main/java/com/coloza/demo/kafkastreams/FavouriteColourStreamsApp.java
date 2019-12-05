package com.coloza.demo.kafkastreams;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Produced;

import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class FavouriteColourStreamsApp {

    private static final String APPLICATION_ID = "favouritecolour-streams-app";
    private static final String SHUTDOWN_HOOK_THREAD_NAME = "favouritecolour-streams-shutdown-hook";
    private String bootstrapServer;

    public FavouriteColourStreamsApp(String bootstrapServer) {
        this.bootstrapServer = bootstrapServer;
    }

    private Properties createStreamsProperties() {
        Properties properties = new Properties();
        properties.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, this.APPLICATION_ID);
        properties.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, this.bootstrapServer);
        properties.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());
        properties.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, OffsetResetStrategy.EARLIEST.name().toLowerCase());
        return properties;
    }

    public void transform(String fromTopic, String toTopic) {
        StreamsBuilder builder = new StreamsBuilder();
        // 1 - get stream from kafka
        KStream<String, String> input = builder.stream(fromTopic);
        KTable<String, Long> count = input
                // 2 - filter bad data
                .filter((defaultKey, line) -> line.contains(","))
                // 3 - select user as key is string at left side of comma character (we discard the old key)
                .selectKey((ignoredKey, line) -> line.split(",")[0].trim().toLowerCase())
                // 4 - map values to get colour as value (string at right side of comma character)
                .mapValues(line -> line.split(",")[1].trim().toLowerCase())
                // 5 - filter keep only colour "green", "red", or "blue"
                .filter((user, colour) -> Arrays.asList("green", "red", "blue").contains(colour))
                // 6 - group by user to discard the old value
                .groupByKey()
                // 7 - discard the old value
                .reduce((oldColour,newColour) -> newColour)
                // 8 - output new stream to continue process data
                .toStream()
                // 9 - select colour as new key (we discard the old key - user)
                .selectKey((user, colour) -> colour)
                // 10 - group by colour before aggregation
                .groupByKey()
                // 11 - <count> occurrences
                .count();
        // 12 - <to> in order to write the results back to kafka
        count.toStream().to(toTopic, Produced.with(Serdes.String(), Serdes.Long()));

        // build the topology and start our streams
        KafkaStreams streams = new KafkaStreams(builder.build(), this.createStreamsProperties());
        CountDownLatch latch = new CountDownLatch(1);

        // shutdown hook to correctly close the streams application
        // attach shutdown handler to catch control Ctrl+C
        Runtime.getRuntime().addShutdownHook(new Thread(this.SHUTDOWN_HOOK_THREAD_NAME){
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

    public void transformWithIntermediaryTopic(String fromTopic, String toTopic) {
        StreamsBuilder builder = new StreamsBuilder();
        // 1 - get stream from kafka
        KStream<String, String> input = builder.stream(fromTopic);
        KStream<String, String> mapping = input
                // 2 - filter bad data
                .filter((defaultKey, line) -> line.contains(","))
                // 3 - select user as key is string at left side of comma character (we discard the old key)
                .selectKey((ignoredKey, line) -> line.split(",")[0].trim().toLowerCase())
                // 4 - map values to get colour as value (string at right side of comma character)
                .mapValues(line -> line.split(",")[1].trim().toLowerCase())
                // 5 - filter keep only colour "green", "red", or "blue"
                .filter((user, colour) -> Arrays.asList("green", "red", "blue").contains(colour));

        String intermediaryTopic = fromTopic + "-mapped";
        mapping.to(intermediaryTopic);

        KTable<String, String> mapped = builder.table(intermediaryTopic);
        KTable<String, Long> count = mapping
                // 6 - select colour as new key (we discard the old key - user)
                .selectKey((user, colour) -> colour)
                // 7 - group by colour before aggregation
                .groupByKey()
                // 8 - <count> occurrences
                .count();
        // 9 - <to> in order to write the results back to kafka
        count.toStream().to(toTopic, Produced.with(Serdes.String(), Serdes.Long()));

        // build the topology and start our streams
        KafkaStreams streams = new KafkaStreams(builder.build(), this.createStreamsProperties());
        CountDownLatch latch = new CountDownLatch(1);

        // shutdown hook to correctly close the streams application
        // attach shutdown handler to catch control Ctrl+C
        Runtime.getRuntime().addShutdownHook(new Thread(this.SHUTDOWN_HOOK_THREAD_NAME){
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
