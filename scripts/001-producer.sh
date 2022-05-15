#!/usr/bin/env bash

# PRODUCER TERMINAL

set -e

CLUSTER_NAME=$(/usr/share/google/get_metadata_value attributes/dataproc-cluster-name)
TITLES_TOPIC=movie-titles
VOTES_TOPIC=movie-rating-votes

# create topics

TOPICS=($TITLES_TOPIC $VOTES_TOPIC)

for topic in ${TOPICS[@]}; do
  /usr/lib/kafka/bin/kafka-topics.sh --create --zookeeper ${CLUSTER_NAME}-m:2181 \
   --replication-factor 1 --partitions 1 --topic ${topic} || true
done


# prepare movie titles directory

rm -rf movie-titles || true
mkdir movie-titles
iconv -f Windows-1250 -t UTF-8 movie_titles.csv > movie-titles/movie_titles.csv


# run producer

java -cp /usr/lib/kafka/libs/*:KafkaProducer.jar \
  com.example.bigdata.TestProducer movie-titles 0 \
  ${TITLES_TOPIC} 1 ${CLUSTER_NAME}-w-0:9092

java -cp /usr/lib/kafka/libs/*:KafkaProducer.jar \
  com.example.bigdata.TestProducer netflix-prize-data 1000000 \
  ${VOTES_TOPIC} 1 ${CLUSTER_NAME}-w-0:9092
