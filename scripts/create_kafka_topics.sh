#!/bin/bash
# Run AFTER starting Kafka: bash scripts/create_kafka_topics.sh
KAFKA_HOME=${KAFKA_HOME:-/opt/kafka}
BS="localhost:9092"
echo "Creating Kafka topics..."
$KAFKA_HOME/bin/kafka-topics.sh --create --if-not-exists \
  --bootstrap-server $BS --topic mess-orders \
  --partitions 3 --replication-factor 1

$KAFKA_HOME/bin/kafka-topics.sh --create --if-not-exists \
  --bootstrap-server $BS --topic weather-feed \
  --partitions 1 --replication-factor 1

$KAFKA_HOME/bin/kafka-topics.sh --create --if-not-exists \
  --bootstrap-server $BS --topic event-feed \
  --partitions 1 --replication-factor 1

echo "Topics created:"
$KAFKA_HOME/bin/kafka-topics.sh --list --bootstrap-server $BS