#!/usr/bin/env bash

# SERVER TERMINAL

CLUSTER_NAME=$(/usr/share/google/get_metadata_value attributes/dataproc-cluster-name)


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

# initialize producers

cat > ~/connect-standalone.properties <<EOL
bootstrap.servers=${CLUSTER_NAME}-w-0:9092
key.converter=org.apache.kafka.connect.storage.StringConverter
value.converter=org.apache.kafka.connect.storage.StringConverter
key.converter.schemas.enable=true
value.converter.schemas.enable=true
internal.key.converter=org.apache.kafka.connect.storage.StringConverter
internal.value.converter=org.apache.kafka.connect.storage.StringConverter
internal.key.converter.schemas.enable=false
internal.value.converter.schemas.enable=false
offset.storage.file.filename=/tmp/connect.offsets
offset.flush.interval.ms=10000
EOL

sudo cp /usr/lib/kafka/config/tools-log4j.properties \
  /usr/lib/kafka/config/connect-log4j.properties
