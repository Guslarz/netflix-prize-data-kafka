#!/usr/bin/env bash

# GCLOUD

CLUSTER_NAME=my-cluster
BUCKET_NAME=guslarz-bucket
PROJECT_ID=put-big-data-2022-02-sk
REGION=europe-west4
ZONE=${REGION}-c

gcloud beta dataproc clusters create ${CLUSTER_NAME} \
    --enable-component-gateway --bucket ${BUCKET_NAME} \
    --region ${REGION} --subnet default --zone ${ZONE} \
    --master-machine-type n1-standard-2 --master-boot-disk-size 50 \
    --num-workers 2 \
    --worker-machine-type n1-standard-2 --worker-boot-disk-size 50 \
    --image-version 2.0-debian10 \
    --optional-components ZOOKEEPER \
    --project ${PROJECT_ID} --max-age=3h \
    --metadata "run-on-master=true" \
    --initialization-actions \
    gs://goog-dataproc-initialization-actions-${REGION}/kafka/kafka.sh

# launch 6 terminals
#   - server terminal
#   - 2 consumers terminals
#   - 2 producers terminals
#   - streaming terminal
