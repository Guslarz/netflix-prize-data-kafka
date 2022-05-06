#!/usr/bin/env bash

# STREAM TERMINAL

set -e

CLUSTER_NAME=$(/usr/share/google/get_metadata_value attributes/dataproc-cluster-name)

java -cp /usr/lib/kafka/libs/*:netflix-prize-data-kafka.jar \
  com.kaczmarek.bigdata.NetflixPrizeDataKafka \
  --server ${CLUSTER_NAME}-w-0:9092 \
  -D 1 -L 2 -O 4
