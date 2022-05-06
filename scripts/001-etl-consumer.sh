#!/usr/bin/env bash

# FIRST CONSUMER TERMINAL

set -e

CLUSTER_NAME=$(/usr/share/google/get_metadata_value attributes/dataproc-cluster-name)
ETL_TOPIC=movie-ratings

/usr/lib/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server ${CLUSTER_NAME}-w-0:9092 \
  --topic $ETL_TOPIC --from-beginning \
  --formatter kafka.tools.DefaultMessageFormatter \
  --property print.key=true \
  --property print.value=true \
  --property key.deserializer=org.apache.kafka.common.serialization.StringDeserializer \
  --property value.deserializer=org.apache.kafka.common.serialization.StringDeserializer
