#!/usr/bin/env bash

# SERVER TERMINAL

set -e

tail -n +2 ~/movie_titles.csv > /tmp/netflix-prize-data/movie_titles.csv

source_dir=~/ratings
target_dir=/tmp/netflix-prize-data/ratings
for file in `ls -v $source_dir/*`; do
  echo $file
  filename=`basename $file`
  cp $file $target_dir/$filename
  sleep 100
done
