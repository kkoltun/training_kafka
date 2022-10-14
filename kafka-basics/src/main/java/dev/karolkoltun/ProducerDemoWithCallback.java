package dev.karolkoltun;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.clients.producer.internals.DefaultPartitioner;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.IntConsumer;
import java.util.stream.IntStream;

import static org.apache.kafka.clients.producer.ProducerConfig.*;

public class ProducerDemoWithCallback {
    private static final Logger log = LoggerFactory.getLogger(ProducerDemoWithCallback.class);

    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.setProperty(BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.setProperty(KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        IntStream.range(0, 10).forEach(i -> sendData(producer, i));

        producer.flush();
        producer.close();
    }

    private static void sendData(KafkaProducer<String, String> kafkaProducer, int i) {
        ProducerRecord<String, String> producerRecord = new ProducerRecord<>("demo_java_2", "Hello world #" + i + "!");
        kafkaProducer.send(producerRecord, ProducerDemoWithCallback::callback);
    }

    private static void callback(RecordMetadata metadata, Exception exception) {
        if (exception == null) {
            log.info(String.format("Received new metadata:\n\tTopic: %s\n\tPartition: %s\n\tOffset: %s\n\tTimestamp: %s",
                    metadata.topic(), metadata.partition(), metadata.offset(), metadata.timestamp()));
        } else {
            log.error(String.format("Received new metadata with error:\tTopic: %s\nPartition: %s\nOffset: %s\nTimestamp: %s",
                    metadata.topic(), metadata.partition(), metadata.offset(), metadata.timestamp()), exception);
        }
    }
}
