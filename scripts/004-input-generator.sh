#!/usr/bin/env bash

# SERVER TERMINAL

set -e

tail -n +2 ~/movie_titles.csv > /tmp/netflix-prize-data/movie_titles.csv

source_dir=~/ratings
target_dir=/tmp/netflix-prize-data/ratings
tmp_dir=/tmp/netflix-prize-data/tmp
sleep_time=10

mkdir -p $tmp_dir

for source_file in `ls -v $source_dir/*`; do
  echo $source_file
  source_basename=`basename $source_file`

  split -l 1000 $source_file ${tmp_dir}/${source_basename}.

  for part in `ls -v $tmp_dir/*`; do
    part_basename=`basename $part`
    echo "  $part_basename"

    cp $part $target_dir/
    sleep $sleep_time
  done
  cd ~/
  rm $tmp_dir/*
done
