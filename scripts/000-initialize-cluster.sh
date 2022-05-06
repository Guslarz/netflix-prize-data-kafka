#!/usr/bin/env bash

# SERVER TERMINAL

set -e

CLUSTER_NAME=$(/usr/share/google/get_metadata_value attributes/dataproc-cluster-name)


# create topics

TITLES_TOPIC=movie-titles
VOTES_TOPIC=movie-rating-votes
ETL_TOPIC=movie-ratings
ANOMALY_TOPIC=popular-movies

TOPICS=($TITLES_TOPIC $VOTES_TOPIC $ETL_TOPIC $ANOMALY_TOPIC)

for topic in ${TOPICS[@]}; do
  /usr/lib/kafka/bin/kafka-topics.sh --create --zookeeper ${CLUSTER_NAME}-m:2181 \
   --replication-factor 1 --partitions 1 --topic ${topic}
done
