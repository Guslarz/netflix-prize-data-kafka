#!/usr/bin/env bash

# TECHNICAL TERMINAL

CLUSTER_NAME=$(/usr/share/google/get_metadata_value attributes/dataproc-cluster-name)
BUCKET_NAME=guslarz-bucket


# download files

hadoop fs -copyToLocal gs://${BUCKET_NAME}/project01/test-input.zip
hadoop fs -copyToLocal gs://${BUCKET_NAME}/project01/netflix-prize-data-kafka.zip

unzip test-input.zip
unzip netflix-prize-data-kafka.zip


# create topics

TITLES_TOPIC=movie-titles
VOTES_TOPIC=movie-rating-votes
ETL_TOPIC=movie-ratings
ANOMALY_TOPIC=popular-movies

TOPICS=($TITLES_TOPIC, $VOTES_TOPIC, $ETL_TOPIC, $ANOMALY_TOPIC)

for topic in ${TOPICS[@]}; do
  /usr/lib/kafka/bin/kafka-topics.sh --create --zookeeper ${CLUSTER_NAME}-m:2181 \
   --replication-factor 1 --partitions 1 --topic ${topic}
done


