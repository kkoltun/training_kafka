package dev.karolkoltun;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.stream.IntStream;

import static org.apache.kafka.clients.producer.ProducerConfig.*;

public class ProducerDemoWithKeys {
    private static final Logger log = LoggerFactory.getLogger(ProducerDemoWithKeys.class);

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
        String topic = "demo_java_2";
        String value = "Hello world #" + i + "!";
        String key = "id_" + i;

        ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topic, key, value);
        kafkaProducer.send(producerRecord, ProducerDemoWithKeys::callback);
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
