package example.kafka.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class App {
    private static final String topic = "fancy-topic";

    public static void main(String[] args) {
        Properties properties = new Properties();
        try (FileInputStream propertiesFile = new FileInputStream("src/main/resources/kafka.properties")) {
            properties.load(propertiesFile);
        } catch (IOException ex) {
            ex.printStackTrace();
        }

        try (Producer<String, String> producer = new KafkaProducer<>(properties)) {
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
                    i++;
                    Thread.sleep(1000);
                } catch (ExecutionException | InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        }
    }
}
