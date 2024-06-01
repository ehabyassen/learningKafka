package io.confluent.streamer;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Produced;

import java.util.Properties;

public class StreamingApp {

    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "even-messages-app");
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());

        StreamsBuilder builder = new StreamsBuilder();
        builder.stream("fancy-topic", Consumed.with(Serdes.String(), Serdes.String()))
                .filter((key, value) -> getMessageNumber(value) % 2 == 0)
                .to("even-messages", Produced.with(Serdes.String(), Serdes.String()));

        KafkaStreams kafkaStreams = new KafkaStreams(builder.build(), properties);
        kafkaStreams.start();

        System.out.println("Listening to topic fancy-topic and writing to topic even-messages...");

        // Add shutdown hook to respond to SIGTERM and gracefully close Kafka Streams
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            kafkaStreams.close();
            kafkaStreams.cleanUp(); // Ensure the state store is cleaned up properly
        }));

        // Keep the application running
        try {
            Thread.sleep(Long.MAX_VALUE);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private static int getMessageNumber(String message) {
        String[] split = message.split("#");
        return Integer.parseInt(split[1]);
    }
}