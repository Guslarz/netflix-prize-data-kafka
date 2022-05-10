#!/usr/bin/env bash

# PRODUCER TERMINAL

set -e

CLUSTER_NAME=$(/usr/share/google/get_metadata_value attributes/dataproc-cluster-name)
TITLES_TOPIC=movie-titles
VOTES_TOPIC=movie-rating-votes

rm -rf /tmp/netflix-prize-data
mkdir -p /tmp/netflix-prize-data
touch /tmp/netflix-prize-data/movie_titles.csv
mkdir /tmp/netflix-prize-data/ratings
mkdir /tmp/netflix-prize-data/errors
mkdir /tmp/netflix-prize-data/finished

mkdir -p /tmp/kafka

wget --no-clobber https://d1i4a15mxbxib1.cloudfront.net/api/plugins/jcustenborder/kafka-connect-spooldir/versions/1.0.42/jcustenborder-kafka-connect-spooldir-1.0.42.zip

unzip jcustenborder-kafka-connect-spooldir-1.0.42.zip -d /tmp/kafka

cat > ~/connect-standalone.properties <<EOL
bootstrap.servers=${CLUSTER_NAME}-w-0:9092
key.converter=org.apache.kafka.connect.storage.StringConverter
value.converter=org.apache.kafka.connect.storage.StringConverter
offset.storage.file.filename=/tmp/connect.offsets
offset.flush.interval.ms=10000
plugin.path=/tmp/kafka
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
connector.class=com.github.jcustenborder.kafka.connect.spooldir.SpoolDirLineDelimitedSourceConnector
tasks.max=1
input.path=/tmp/netflix-prize-data/ratings
input.file.pattern=^.*\\.csv$
topic=${VOTES_TOPIC}
error.path=/tmp/netflix-prize-data/errors
finished.path=/tmp/netflix-prize-data/finished
EOL

sudo cp /usr/lib/kafka/config/tools-log4j.properties \
  /usr/lib/kafka/config/connect-log4j.properties

export CLASSPATH="/usr/lib/kafka/libs:/tmp/kafka/jcustenborder-kafka-connect-spooldir-1.0.42/lib"

/usr/lib/kafka/bin/connect-standalone.sh \
  connect-standalone.properties \
  connect-titles-source.properties \
  connect-ratings-source.properties
