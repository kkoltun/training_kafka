package dev.karolkoltun;

import com.launchdarkly.eventsource.EventHandler;
import com.launchdarkly.eventsource.MessageEvent;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WikimediaChangeHandler implements EventHandler {

    private static final Logger log = LoggerFactory.getLogger(WikimediaChangeHandler.class);

    private final String topic;
    private final KafkaProducer<String, String> producer;

    public WikimediaChangeHandler(String topic, KafkaProducer<String, String> producer) {
        this.topic = topic;
        this.producer = producer;
    }

    @Override
    public void onOpen() {
        log.info("onOpen");
    }

    @Override
    public void onClosed() {
        log.info("onClosed");
        producer.close();
    }

    @Override
    public void onMessage(String event, MessageEvent messageEvent) {
        ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topic, messageEvent.getData());
        producer.send(producerRecord);
    }

    @Override
    public void onComment(String comment) {
        log.info("onComment");
    }

    @Override
    public void onError(Throwable t) {
        log.info("onError", t);
    }
}
