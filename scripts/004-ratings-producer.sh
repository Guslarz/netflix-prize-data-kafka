#!/usr/bin/env bash

# SECOND PRODUCER TERMINAL

mkdir -p /tmp/netflix-prize-data
touch /tmp/netflix-prize-data/ratings.csv

cat > ~/connect-ratings-source.properties <<EOL
name=local-file-source
connector.class=FileStreamSource
tasks.max=1
file=/tmp/netflix-prize-data/ratings.csv
topic=apache-logs
EOL

/usr/lib/kafka/bin/connect-standalone.sh \
  connect-standalone.properties \
  connect-titles-source.properties
