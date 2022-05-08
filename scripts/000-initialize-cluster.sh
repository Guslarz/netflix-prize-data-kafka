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
docker run --name=kafka-mysql -p8081:3306 -e MYSQL_ROOT_PASSWORD=password -d mysql/mysql-server:latest
docker exec -it kafka-mysql bash <<EOL
mysql -u root -p
password

UPDATE mysql.user SET host = '%' WHERE user='root';

CREATE DATABASE netflix_prize_data;

USE netflix_prize_data;

CREATE TABLE movie_rating_votes(
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
