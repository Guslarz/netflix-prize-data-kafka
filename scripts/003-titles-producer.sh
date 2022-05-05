#!/usr/bin/env bash

# FIRST PRODUCER TERMINAL

cat > ~/connect-titles-source.properties <<EOL
name=local-file-source
connector.class=FileStreamSource
tasks.max=1
file=~/movie-titles.csv
topic=apache-logs
EOL

/usr/lib/kafka/bin/connect-standalone.sh \
  connect-standalone.properties \
  connect-titles-source.properties
