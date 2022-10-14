package dev.karolkoltun.processor;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.ValueMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class BotCountSteamBuilder {
    private static final Logger log = LoggerFactory.getLogger(BotCountSteamBuilder.class);

    private static final String BOT_COUNT_STORE = "bot-count-store";
    private static final String BOT_COUNT_TOPIC = "bot-count-topic";
    private final KStream<String, String> inputStream;
    private final ObjectMapper objectMapper;

    public BotCountSteamBuilder(KStream<String, String> inputStream) {
        this.inputStream = inputStream;
        this.objectMapper = new ObjectMapper();
    }

    public void setup() {
        inputStream.mapValues(value -> {
                    try {
                        String payload = objectMapper.readTree(value).get("payload").textValue();

                        return objectMapper.readTree(payload).get("bot").asBoolean()
                                ? "bot"
                                : "non-bot";
                    } catch (Exception e) {
                        log.error(String.format("error parsing [%s]", value), e);
                        return "unknown";
                    }
                })
                .peek((key, value) -> log.info(String.format("Peek 1: key [%s]; value [%s].", key, value)))
                .groupBy((key, isBot) -> isBot)
                .count(Materialized.as(BOT_COUNT_STORE))
                .toStream()
                .peek((key, value) -> log.info(String.format("Peek 2: key [%s]; value [%s].", key, value)))
                .mapValues((key, value) -> {
                    Map<String, Long> keyValue = Map.of(key, value);
                    try {
                        return objectMapper.writeValueAsString(keyValue);
                    } catch (JsonProcessingException e) {
                        throw new RuntimeException(e);
                    }
                })
                .peek((key, value) -> log.info(String.format("Peek 3: key [%s]; value [%s].", key, value)))
                .to(BOT_COUNT_TOPIC);
    }
}
