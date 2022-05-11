#!/usr/bin/env bash

# CONSUMER TERMINAL

set -e

CLUSTER_NAME=$(/usr/share/google/get_metadata_value attributes/dataproc-cluster-name)
ETL_TOPIC=movie-ratings
ANOMALY_TOPIC=popular-movies
CONNECTION_URL="jdbc:mysql://127.0.0.1:8080/netflix_prize_data?verifyServerCertificate=false&useSSL=true&requireSSL=true"
CONNECTION_USER=root
CONNECTION_PASSWORD=password
ETL_TABLE=movie_ratings
ANOMALY_TABLE=popular_movies


# install kafka-connect-jdbc

rm confluentinc-kafka-connect-jdbc-10.4.1.zip || true
wget https://d1i4a15mxbxib1.cloudfront.net/api/plugins/confluentinc/kafka-connect-jdbc/versions/10.4.1/confluentinc-kafka-connect-jdbc-10.4.1.zip

rm -rf /tmp/kafka/confluentinc-kafka-connect-jdbc-10.4.1 || true
unzip confluentinc-kafka-connect-jdbc-10.4.1.zip -d /tmp/kafka


# install mysql driver

rm mysql-connector-java-8.0.29.tar.gz || true
wget https://dev.mysql.com/get/Downloads/Connector-J/mysql-connector-java-8.0.29.tar.gz

tar -xzvf mysql-connector-java-8.0.29.tar.gz
cp mysql-connector-java-8.0.29/mysql-connector-java-8.0.29.jar /tmp/kafka/confluentinc-kafka-connect-jdbc-10.4.1


# create properties files

cat > ~/consumer-connector.properties <<EOL
bootstrap.servers=${CLUSTER_NAME}-w-0:9092
key.converter=org.apache.kafka.connect.json.JsonConverter
key.converter.schemas.enable=true
value.converter=org.apache.kafka.connect.json.JsonConverter
value.converter.schemas.enable=true
offset.storage.file.filename=/tmp/consumer.offsets
offset.flush.interval.ms=10000
plugin.path=/tmp/kafka
rest.port=8083
EOL

cat > ~/connect-etl-sink.properties <<EOL
name=etl-sink
connector.class=io.confluent.connect.jdbc.JdbcSinkConnector
tasks.max=1
topics=${ETL_TOPIC}
connection.url=${CONNECTION_URL}
connection.user=${CONNECTION_USER}
connection.password=${CONNECTION_PASSWORD}
dialect.name=MySqlDatabaseDialect
insert.mode=upsert
table.name.format=${ETL_TABLE}
pk.mode=record_key
pk.fields=
EOL

cat > ~/connect-anomaly-sink.properties <<EOL
name=anomaly-sink
connector.class=io.confluent.connect.jdbc.JdbcSinkConnector
tasks.max=1
topics=${ANOMALY_TOPIC}
connection.url=${CONNECTION_URL}
connection.user=${CONNECTION_USER}
connection.password=${CONNECTION_PASSWORD}
dialect.name=MySqlDatabaseDialect
insert.mode=upsert
table.name.format=${ANOMALY_TABLE}
pk.mode=record_key
pk.fields=
EOL


# run connector

export CLASSPATH="/usr/lib/kafka/libs:/tmp/kafka/confluentinc-kafka-connect-jdbc-10.4.1/lib://tmp/kafka/confluentinc-kafka-connect-jdbc-10.4.1"

/usr/lib/kafka/bin/connect-standalone.sh \
  consumer-connector.properties \
  connect-etl-sink.properties \
  connect-anomaly-sink.properties
