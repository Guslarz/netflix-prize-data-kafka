#!/usr/bin/env bash

# SERVER TERMINAL

source_dir="~/ratings/*"
target_file="/tmp/netflix-prize-data/ratings.csv"
for file in $source_dir; do
  cat $file >> $target_file
  sleep 1000
done
