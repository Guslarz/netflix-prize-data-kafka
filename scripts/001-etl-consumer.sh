#!/usr/bin/env bash

# FIRST CONSUMER TERMINAL

ETL_TOPIC=movie-ratings

bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:6667 \
  --topic $ETL_TOPIC --from-beginning \
  --formatter kafka.tools.DefaultMessageFormatter \
  --property print.key=true \
  --property print.value=true \
  --property key.deserializer=org.apache.kafka.common.serialization.StringDeserializer \
  --property value.deserializer=org.apache.kafka.common.serialization.StringDeserializer
