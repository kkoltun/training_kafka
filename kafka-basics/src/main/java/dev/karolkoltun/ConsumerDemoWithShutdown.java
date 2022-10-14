package dev.karolkoltun;

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

import static org.apache.kafka.clients.consumer.ConsumerConfig.*;

public class ConsumerDemoWithShutdown {
    private static final Logger log = LoggerFactory.getLogger(ConsumerDemoWithShutdown.class);

    public static void main(String[] args) {
        String bootstrapServer = "localhost:9092";
        String groupId = "karolkoltundev-application-second";
        String topic = "demo_java_2";

        Properties properties = new Properties();
        properties.setProperty(BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        properties.setProperty(KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(GROUP_ID_CONFIG, groupId);
        properties.setProperty(AUTO_OFFSET_RESET_CONFIG, "earliest");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

        final Thread mainThread = Thread.currentThread();
        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                log.info("Detected a shutdown, exit consumer.");
                // This will make the consumer throw a WakeupException on next poll() invocation.
                consumer.wakeup();

                // Join the main thread.
                try {
                    mainThread.join();
                } catch (InterruptedException e) {
                    log.error("Interrupted", e);
                }
            }
        });

        try {
            consumer.subscribe(Collections.singletonList(topic));

            while (true) {
                log.info("Polling");
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

                for (var record : records) {
                    log.info(String.format("Key: %s; value: %s; partition: %s; offset: %s.",
                            record.key(), record.value(), record.partition(), record.offset()));
                }
            }
        } catch (WakeupException wakeupException) {
            log.info("Wakeup exception", wakeupException);
        } catch (Exception exception) {
            log.error("Unexpected error", exception);
        } finally {
            log.info("Closing the consumer.");
            // This will close the consumer gracefully - allowing the consumer in a group to do proper rebalance.
            // Also, this commits offsets.
            consumer.close();
        }
    }
}
