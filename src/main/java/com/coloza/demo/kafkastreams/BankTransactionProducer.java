package com.coloza.demo.kafkastreams;

import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Instant;
import java.util.Properties;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

public class BankTransactionProducer {

    private String bootstrapServer;
    private String targetTopic;

    public BankTransactionProducer(String bootstrapServer, String targetTopic) {
        this.bootstrapServer = bootstrapServer;
        this.targetTopic = targetTopic;
    }

    public static void main(String[] args) {
        BankTransactionProducer app = new BankTransactionProducer(
                args.length >= 1 ? args[0] : "localhost:9092",
                args.length >= 2 ? args[1] : "streams-bank-transaction-input"
        );
        Producer<String, String> producer = new KafkaProducer<>(app.createProperties());
        int i = 0;
        while (true) {
            System.out.println("Producing batch: " + i);
            try {
                producer.send(app.newRandomTransaction("max"));
                TimeUnit.MILLISECONDS.sleep(100);
                producer.send(app.newRandomTransaction("john"));
                TimeUnit.MILLISECONDS.sleep(100);
                producer.send(app.newRandomTransaction("alice"));
                TimeUnit.MILLISECONDS.sleep(100);
                ++i;
            } catch (InterruptedException e) {
                break;
            }
        }
        producer.close();
    }

    private Properties createProperties() {
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, this.bootstrapServer);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.ACKS_CONFIG, "all"); // strongest producing guarantee
        properties.setProperty(ProducerConfig.RETRIES_CONFIG, "3"); // we don't want to lose data
        properties.setProperty(ProducerConfig.LINGER_MS_CONFIG, "1");
        properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true"); // ensure we don't push duplicates
        return properties;
    }

    private ProducerRecord<String, String> newRandomTransaction(String name) {
        ObjectNode transaction = JsonNodeFactory.instance.objectNode();
        transaction.put("name", name);
        transaction.put("amount", ThreadLocalRandom.current().nextInt(0, 100));
        transaction.put("time", Instant.now().toString());
        return new ProducerRecord<>(targetTopic, name, transaction.toString());
    }
}
