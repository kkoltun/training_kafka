# Kafka training project

Example code for various features from Kafka.

### TODO

Build an entire structure with Kafka Connect, Kafka Stream, Kafka Schema Registry, put everything into a docker container structure.

1. Kafka Connect Wikimedia Source -> Topic wikimedia.recentchange
2. Topic wikimedia.recentchange -> Kafka Stream map to Avro -> Topic wikimedia.recentchange.mapped
3. Topic wikimedia.recentchange.mapped -> Kafka Connect Elasticsearch Sink
4. Topic wikimedia.recentchange.mapped -> Postgres database table (Kafka Connect Sink?)

Example mapped content:
{
    "id": 1559087321,
    "type": "edit",
    "title": "Sonderkommando Rote Kapelle",
    "comment": "cite",
    "timestamp": 1665602005,
    "user": "Scope creep",
    "bot": false,
    "server_url": "https://en.wikipedia.org",
    "server_name": "en.wikipedia.org"
}