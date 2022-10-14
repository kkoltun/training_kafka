package dev.karolkoltun;

import com.launchdarkly.eventsource.EventSource;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.net.URISyntaxException;
import java.time.Duration;
import java.util.Properties;

import static java.util.concurrent.TimeUnit.MINUTES;
import static org.apache.kafka.clients.producer.ProducerConfig.*;

public class WikimediaChangesProducer {

    private static final Logger log = LoggerFactory.getLogger(WikimediaChangeHandler.class);
    private static final String URI = "https://stream.wikimedia.org/v2/stream/recentchange";
    private static final String TOPIC = "wikimedia.recentchange";

    public static void main(String[] args) throws URISyntaxException {
        Properties properties = new Properties();
        properties.setProperty(BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.setProperty(KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // High throughput producer config
        properties.setProperty(LINGER_MS_CONFIG, "20");
        properties.setProperty(BATCH_SIZE_CONFIG, Integer.toString(32 * 1024));
        properties.setProperty(COMPRESSION_TYPE_CONFIG, "snappy");

        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        WikimediaChangeHandler changeHandler = new WikimediaChangeHandler(TOPIC, producer);
        EventSource.Builder eventSourceBuilder = new EventSource.Builder(changeHandler, new URI(URI));
        EventSource eventSource = eventSourceBuilder.build();
        eventSource.start();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> shutdownHook(eventSource)));

        log.info("waiting for shutdown");
        try {
            MINUTES.sleep(10);
        } catch (InterruptedException exception) {
            log.info("woken up");
        }
    }

    private static void shutdownHook(EventSource eventSource) {
        log.info("detected a shutdown.");

        eventSource.close();

        try {
            log.info("await closed");
            eventSource.awaitClosed(Duration.ofSeconds(5));
        } catch (InterruptedException e) {
            log.error("interrupted", e);
        }

        log.info("awaited");
    }
}
