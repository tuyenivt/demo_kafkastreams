package com.coloza.demo.kafkastreams;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.test.ConsumerRecordFactory;
import org.apache.kafka.streams.test.OutputVerifier;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Properties;

public class WordCountStreamsAppTests {

    private static final String APPLICATION_ID = "test";
    private static final String BOOTSTRAP_SERVERS = "dummy:1234";
    private static final String INPUT_TOPIC = "streams-plaintext-input";
    private static final String OUTPUT_TOPIC = "streams-wordcount-output";

    private TopologyTestDriver testDriver;
    private ConsumerRecordFactory<String, String> recordFactory = new ConsumerRecordFactory<>(new StringSerializer(), new StringSerializer());
    private StringDeserializer stringDeserializer = new StringDeserializer();
    private LongDeserializer longDeserializer = new LongDeserializer();

    @BeforeEach
    public void setup() {
        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, APPLICATION_ID);
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        WordCountStreamsApp app = new WordCountStreamsApp();
        testDriver = new TopologyTestDriver(app.createTopology(INPUT_TOPIC, OUTPUT_TOPIC), config);
    }

    @AfterEach
    public void tearDown() {
        testDriver.close();
    }

    private void pushNewInputRecord(String value) {
        testDriver.pipeInput(recordFactory.create(INPUT_TOPIC, null, value));
    }

    private ProducerRecord<String, Long> readOutput() {
        return testDriver.readOutput(OUTPUT_TOPIC, stringDeserializer, longDeserializer);
    }

    @Test
    public void testCount() {
        pushNewInputRecord("testing kafka streams");
        OutputVerifier.compareKeyValue(readOutput(), "testing", 1L);
        OutputVerifier.compareKeyValue(readOutput(), "kafka", 1L);
        OutputVerifier.compareKeyValue(readOutput(), "streams", 1L);
        Assertions.assertNull(readOutput());

        pushNewInputRecord("testing kafka again");
        OutputVerifier.compareKeyValue(readOutput(), "testing", 2L);
        OutputVerifier.compareKeyValue(readOutput(), "kafka", 2L);
        OutputVerifier.compareKeyValue(readOutput(), "again", 1L);
        Assertions.assertNull(readOutput());
    }
}
