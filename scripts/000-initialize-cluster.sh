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


# initialize database

docker pull mysql/mysql-server:latest
docker run --name=kafka-mysql -p8080:3306 \
  -e MYSQL_ROOT_PASSWORD=password \
  -e MYSQL_ROOT_HOST=% \
  -d mysql/mysql-server:latest
echo "Wait..."
sleep 30
mysql -h 127.0.0.1 -P 8080 -u root -ppassword <<EOL
CREATE DATABASE netflix_prize_data;

USE netflix_prize_data;

CREATE TABLE movie_ratings(
  movieId INT NOT NULL,
  yearMonth CHAR(7) NOT NULL,
  title VARCHAR(255) NOT NULL,
  voteCount INT NOT NULL,
  ratingSum INT NOT NULL,
  uniqueVoterCount INT NOT NULL,
  PRIMARY KEY(movieId, yearMonth)
);

CREATE TABLE popular_movies(
    movieId INT,
    windowStart CHAR(10),
    windowEnd CHAR(10),
    title VARCHAR(255),
    voteCount INT,
    ratingAverage DOUBLE(3, 2),
    PRIMARY KEY(movieId, windowStart, windowEnd)
);
EOL


# copy log4j config

sudo cp /usr/lib/kafka/config/tools-log4j.properties \
  /usr/lib/kafka/config/connect-log4j.properties
