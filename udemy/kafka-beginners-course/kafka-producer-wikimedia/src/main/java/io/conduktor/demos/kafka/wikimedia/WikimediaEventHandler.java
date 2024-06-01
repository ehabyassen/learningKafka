package io.conduktor.demos.kafka.wikimedia;

import com.google.gson.JsonParser;
import com.launchdarkly.eventsource.EventHandler;
import com.launchdarkly.eventsource.MessageEvent;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WikimediaEventHandler implements EventHandler {

    private static final Logger log = LoggerFactory.getLogger(WikimediaEventHandler.class.getSimpleName());
    private final Producer<String, String> kafkaProducer;
    private final String topicName;

    public WikimediaEventHandler(Producer<String, String> kafkaProducer, String topicName) {
        this.kafkaProducer = kafkaProducer;
        this.topicName = topicName;
    }

    @Override
    public void onOpen() {
        // do nothing
    }

    @Override
    public void onClosed() {
        kafkaProducer.close();
    }

    @Override
    public void onMessage(String event, MessageEvent messageEvent) {
        String message = messageEvent.getData();
        log.info(String.format("Sending message. Id: %s, message: %s", extractId(message), message));
        kafkaProducer.send(new ProducerRecord<>(topicName, message));
    }

    @Override
    public void onComment(String s) {
        // do nothing
    }

    @Override
    public void onError(Throwable throwable) {
        log.error("Error on reading stream: ", throwable);
    }

    private static String extractId(String value) {
        return JsonParser.parseString(value)
                .getAsJsonObject()
                .get("meta")
                .getAsJsonObject()
                .get("id")
                .getAsString();
    }
}
