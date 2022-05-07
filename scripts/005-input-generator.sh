#!/usr/bin/env bash

# SERVER TERMINAL

set -e

tail -n +2 ~/movie_titles.csv >> /tmp/netflix-prize-data/movie_titles.csv

source_files=~/ratings
target_file=/tmp/netflix-prize-data/ratings.csv
for file in `ls -v $source_files/*`; do
  echo $file
  cat $file >> $target_file
  sleep 1
done
