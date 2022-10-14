package dev.karolkoltun;

import com.google.gson.JsonParser;
import org.apache.http.HttpHost;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.kafka.clients.consumer.ConsumerConfig.*;
import static org.elasticsearch.client.RequestOptions.DEFAULT;

public class ElasticSearchConsumer {
    private static final Logger log = LoggerFactory.getLogger(ElasticSearchConsumer.class);

    private static final String TOPIC = "wikimedia.recentchange";

    public static void main(String[] args) {
        RestHighLevelClient openSearchClient = createOpenSearchClient();
        KafkaConsumer<String, String> kafkaConsumer = createKafkaConsumer();

        try (openSearchClient; kafkaConsumer) {
            String indexName = "wikimedia";
            createIndexIfMissing(openSearchClient, indexName);

            Runtime.getRuntime().addShutdownHook(new Thread(() -> shutdownHook(kafkaConsumer, Thread.currentThread())));

            kafkaConsumer.subscribe(Collections.singletonList(TOPIC));

            while (true) {
                ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.ofMillis(3000));
                int recordCount = records.count();

                log.info(String.format("Received [%s] records.", recordCount));

                BulkRequest bulkRequest = new BulkRequest();
                for (ConsumerRecord<String, String> record : records) {
                    try {
                        IndexRequest indexRequest = new IndexRequest(indexName)
                                .source(record.value(), XContentType.JSON)
                                .id(extractId(record.value()));
                        bulkRequest.add(indexRequest);
                    } catch (Exception exception) {
                        log.info(String.format("Failed to insert the document on [%s]. Ignoring.", exception.getMessage()));
                    }
                }

                if (bulkRequest.numberOfActions() <= 0) {
                    continue;
                }

                BulkResponse bulkResponse = openSearchClient.bulk(bulkRequest, DEFAULT);
                log.info(String.format("Bulk inserted [%s] items.", bulkResponse.getItems().length));

                kafkaConsumer.commitSync();

                // Increase the chance of a bulk action.
                SECONDS.sleep(1);
            }
        } catch (WakeupException wakeupException) {
            log.info("Wakeup exception", wakeupException);
        } catch (Exception exception) {
            log.error("Unexpected error", exception);
        }
    }

    private static void createIndexIfMissing(RestHighLevelClient openSearchClient, String indexName) throws IOException {
        GetIndexRequest getIndexRequest = new GetIndexRequest(indexName);
        boolean wikimediaIndexExists = openSearchClient.indices().exists(getIndexRequest, DEFAULT);
        if (!wikimediaIndexExists) {
            log.info(String.format("Creating [%s] index", indexName));

            CreateIndexRequest createIndexRequest = new CreateIndexRequest(indexName);
            openSearchClient.indices().create(createIndexRequest, DEFAULT);
        } else {
            log.info(String.format("Index [%s] already exists", indexName));
        }
    }

    public static RestHighLevelClient createOpenSearchClient() {
        URI uri = URI.create("http://localhost:9200");
        HttpHost httpHost = new HttpHost(uri.getHost(), uri.getPort(), "http");
        RestClientBuilder restClientBuilder = RestClient.builder(httpHost);
        return new RestHighLevelClient(restClientBuilder);
    }

    private static KafkaConsumer<String, String> createKafkaConsumer() {
        Properties properties = new Properties();
        properties.setProperty(BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.setProperty(KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(GROUP_ID_CONFIG, "kkdev-2");
        properties.setProperty(AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.setProperty(ENABLE_AUTO_COMMIT_CONFIG, "false");

        return new KafkaConsumer<>(properties);
    }

    private static String extractId(String json) {
        return JsonParser.parseString(json)
                .getAsJsonObject()
                .get("meta")
                .getAsJsonObject()
                .get("id")
                .getAsString();
    }

    private static void shutdownHook(KafkaConsumer<String, String> consumer, Thread mainThread) {
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
}
