package example.kafka.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class App {

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("auto.create.topics.enable", false);
        props.put("acks", "all");

        try (Producer<String, String> producer = new KafkaProducer<>(props)) {
            int i = 0;
            while (true) {
                try {
                    String key = String.valueOf(i);
                    String value = "message #" + i;
                    String topic = "fancy-topic";
                    Future<RecordMetadata> responseFuture = producer.send(new ProducerRecord<>(topic, key, value));
                    RecordMetadata response = responseFuture.get();
                    int partition = response.partition();
                    long offset = response.offset();
                    System.out.println("Sent message: " + value + ", partition: " + partition + ", offset: " + offset);
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
