#!/usr/bin/env bash

# SERVER TERMINAL

BUCKET_NAME=guslarz-bucket


# download files

hadoop fs -copyToLocal gs://${BUCKET_NAME}/project01/netflix-prize-data-input.zip
hadoop fs -copyToLocal gs://${BUCKET_NAME}/project01/netflix-prize-data-executables.zip

unzip netflix-prize-data-input.zip
unzip netflix-prize-data-executables.zip


# set permissions

chmod +x scripts/*
