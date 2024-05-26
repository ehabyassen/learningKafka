package example.kafka.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class App {
    private static final String kafkaHost = "localhost:9092";
    private static final String topic = "fancy-topic";

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put("bootstrap.servers", kafkaHost);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("auto.create.topics.enable", false);
        props.put("allow.auto.create.topics", false);
        props.put("acks", "all");

        try (Producer<String, String> producer = new KafkaProducer<>(props)) {
            int i = 0;
            while (true) {
                try {
                    String key = String.valueOf(i);
                    String value = String.format("message #%d", i);
                    Future<RecordMetadata> responseFuture = producer.send(new ProducerRecord<>(topic, key, value));
                    RecordMetadata response = responseFuture.get();
                    int partition = response.partition();
                    long offset = response.offset();
                    String log = String.format("Sent message: %s, partition: %d, offset: %d", value, partition, offset);
                    System.out.println(log);
                } catch (InterruptedException | ExecutionException e) {
                    throw new RuntimeException(e);
                }
                i++;
                Thread.sleep(1000);
            }
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
}
