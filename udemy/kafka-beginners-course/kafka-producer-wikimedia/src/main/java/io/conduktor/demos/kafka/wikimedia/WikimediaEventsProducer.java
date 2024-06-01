package io.conduktor.demos.kafka.wikimedia;

import com.launchdarkly.eventsource.EventSource;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;

import java.net.URI;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class WikimediaEventsProducer {

    private static final String bootstrapServers = "localhost:9092";
    private static final String topicName = "wikimedia.recentchange";
    private static final String changesUrl = "https://stream.wikimedia.org/v2/stream/recentchange";

    public static void main(String[] args) throws InterruptedException {
        Properties properties = getProperties();
        try (Producer<String, String> producer = new KafkaProducer<>(properties);
             EventSource eventSource = new EventSource.Builder(new WikimediaEventHandler(producer, topicName),
                     URI.create(changesUrl)).build()) {
            eventSource.start();
            // produce for 10 minutes
            TimeUnit.MINUTES.sleep(1);
        }
    }

    private static Properties getProperties() {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        //high throughput producer config
        properties.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");
        properties.put(ProducerConfig.LINGER_MS_CONFIG, "20");
        properties.put(ProducerConfig.BATCH_SIZE_CONFIG, String.valueOf(32 * 1024));
        return properties;
    }
}
