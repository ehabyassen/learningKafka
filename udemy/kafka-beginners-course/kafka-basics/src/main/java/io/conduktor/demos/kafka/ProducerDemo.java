package io.conduktor.demos.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class ProducerDemo {

    private static final Logger log = LoggerFactory.getLogger(ProducerDemo.class.getSimpleName());
    private static final String topicName = "first-topic";

    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "localhost:9092");
        properties.put("key.serializer", StringSerializer.class.getName());
        properties.put("value.serializer", StringSerializer.class.getName());
        properties.put("acks", "all");

        try (Producer<String, String> producer = new KafkaProducer<>(properties)) {
            int i = 0;
            while (i < 10_000) {
                String key = String.valueOf(i);
                String value = String.format("message #%d", i);
                ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topicName, key, value);
                Future<RecordMetadata> responseFuture = producer.send(producerRecord);
                RecordMetadata response = responseFuture.get();
                int partition = response.partition();
                long offset = response.offset();
                log.info(String.format("sent message: %s, partition: %d, offset: %d", value, partition, offset));
                Thread.sleep(1000);
                i++;
            }
        } catch (ExecutionException | InterruptedException e) {
            log.error(e.getMessage());
        }
    }
}
