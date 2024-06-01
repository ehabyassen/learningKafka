package io.confluent.consumer;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.io.FileInputStream;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.Collections;
import java.util.Date;
import java.util.Properties;
import java.util.Random;

public class ConsumerApp {
    private static final String topic = "fancy-topic";

    public static void main(String[] args) {
        Properties properties = new Properties();
        try (FileInputStream propertiesFile = new FileInputStream("src/main/resources/kafkaConsumer.properties")) {
            properties.load(propertiesFile);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        try (Consumer<String, String> consumer = new KafkaConsumer<>(properties)) {
            consumer.subscribe(Collections.singletonList(topic));
            System.out.printf("Listening to topic %s...\n", topic);

            int i = 0;
            while (i < 10_000) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(10));
                for (ConsumerRecord<String, String> msg : records) {
                    System.out.printf("Consumed message: %s partition: %s at: %s\n", msg.value(), msg.partition(),
                            new SimpleDateFormat("ss:SSS").format(new Date()));
                    Thread.sleep(new Random().nextInt(100));
                }
                i++;
            }
        } catch (Exception e) {
            System.out.println("Error consuming.." + e);
        }
    }
}