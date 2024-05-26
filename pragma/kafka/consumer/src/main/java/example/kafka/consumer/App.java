package example.kafka.consumer;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.Collections;
import java.util.Date;
import java.util.Properties;
import java.util.Random;

public class App {
    private static final String kafkaHost = "localhost:9092";
    private static final String topic = "fancy-topic";
    private static final String groupName = "consumer-group1";

    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", kafkaHost);
        properties.put("key.deserializer",
                "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer",
                "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("group.id", groupName);

        try (Consumer<String, String> consumer = new KafkaConsumer<>(properties)) {
            consumer.subscribe(Collections.singletonList(topic));
            System.out.printf("Listening to topic %s...\n", topic);

            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(10));
                for (ConsumerRecord<String, String> msg : records) {
                    System.out.printf("Consumed message: %s partition %s at %s\n", msg.value(),
                            msg.partition(), new SimpleDateFormat("ss:SSS").format(new Date()));
                    Thread.sleep(new Random().nextInt(100));
                }
            }
        } catch (Exception e) {
            System.out.println("Error consuming.." + e);
        }
    }
}
