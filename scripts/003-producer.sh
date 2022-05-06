#!/usr/bin/env bash

# PRODUCER TERMINAL

set -e

CLUSTER_NAME=$(/usr/share/google/get_metadata_value attributes/dataproc-cluster-name)
TITLES_TOPIC=movie-titles
VOTES_TOPIC=movie-rating-votes

rm -rf /tmp/netflix-prize-data
mkdir -p /tmp/netflix-prize-data
touch /tmp/netflix-prize-data/movie_titles.csv
touch /tmp/netflix-prize-data/ratings.csv

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

cat > ~/connect-titles-source.properties <<EOL
name=titles-file-source
connector.class=FileStreamSource
tasks.max=1
file=/tmp/netflix-prize-data/movie_titles.csv
topic=${TITLES_TOPIC}
EOL

cat > ~/connect-ratings-source.properties <<EOL
name=ratings-file-source
connector.class=FileStreamSource
tasks.max=1
file=/tmp/netflix-prize-data/ratings.csv
topic=${VOTES_TOPIC}
EOL

sudo cp /usr/lib/kafka/config/tools-log4j.properties \
  /usr/lib/kafka/config/connect-log4j.properties

/usr/lib/kafka/bin/connect-standalone.sh \
  connect-standalone.properties \
  connect-titles-source.properties \
  connect-ratings-source.properties
