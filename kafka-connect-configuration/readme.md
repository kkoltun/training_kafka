Kafka Connect Source Wikimedia https://github.com/conduktor/kafka-connect-wikimedia
Kafka Connect Sink Elasticsearch https://www.confluent.io/hub/confluentinc/kafka-connect-elasticsearch

1. Download both, extract here.
2. Edit the properties (paths, topic names).
3. Launch using kafka scripts `[PATH-TO-KAFKA]\bin\windows\connect-standalone.bat connect-standalone.properties elasticsearch.properties wikimedia.properties