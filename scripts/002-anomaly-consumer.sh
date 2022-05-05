#!/usr/bin/env bash

# SECOND CONSUMER TERMINAL

ANOMALY_TOPIC=popular-movies

bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:6667 \
  --topic $ANOMALY_TOPIC --from-beginning \
  --formatter kafka.tools.DefaultMessageFormatter \
  --property print.key=true \
  --property print.value=true \
  --property key.deserializer=org.apache.kafka.common.serialization.StringDeserializer \
  --property value.deserializer=org.apache.kafka.common.serialization.StringDeserializer
