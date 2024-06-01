package io.confluent.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.Future;

public class ProducerApp {
    private static final String topic = "fancy-topic";

    public static void main(String[] args) {
        Properties properties = new Properties();
        try (FileInputStream propertiesFile = new FileInputStream("src/main/resources/kafkaProducer.properties")) {
            properties.load(propertiesFile);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        try (Producer<String, String> producer = new KafkaProducer<>(properties)) {
            int i = 0;
            while (i < 10_000) {
                try {
                    String key = String.valueOf(i);
                    String value = String.format("message #%d", i);
                    Future<RecordMetadata> responseFuture = producer.send(new ProducerRecord<>(topic, key, value));
                    RecordMetadata response = responseFuture.get();
                    int partition = response.partition();
                    long offset = response.offset();
                    System.out.printf("Sent message: %s, partition: %d, offset: %d\n", value, partition, offset);
                    i++;
                    Thread.sleep(1000);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
        }
    }
}