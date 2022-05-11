# Netflix Prize Data - Kafka Streams

## 1. Build Project
Run `mvn clean install -DskipTests`

## 2. Create cluster

Create Dataproc cluster with Zookeeper and Docker, e.g.:
```shell
CLUSTER_NAME=...
BUCKET_NAME=...
PROJECT_ID=...
REGION=europe-west4
ZONE=${REGION}-c

gcloud beta dataproc clusters create ${CLUSTER_NAME} \
    --enable-component-gateway --bucket ${BUCKET_NAME} \
    --region ${REGION} --subnet default --zone ${ZONE} \
    --master-machine-type n1-standard-2 --master-boot-disk-size 50 \
    --num-workers 2 \
    --worker-machine-type n1-standard-2 --worker-boot-disk-size 50 \
    --image-version 2.0-debian10 \
    --optional-components ZOOKEEPER,DOCKER \
    --project ${PROJECT_ID} --max-age=3h \
    --metadata "run-on-master=true" \
    --initialization-actions \
    gs://goog-dataproc-initialization-actions-${REGION}/kafka/kafka.sh
```

Then launch 5 terminals:
- technical terminal
- server terminal
- consumer terminal
- producer terminal
- streaming terminal

## 3. Download files (server terminal)

Copy to cluster:
- netflix-prize-data-executables.zip - unzip in home directory
- movie_titles.csv - copy to home directory
- netflix-prize-data.zip - 
download from [here](https://www.cs.put.poznan.pl/kjankiewicz/bigdata/stream_project/netflix-prize-data.zip), 
unzip to have all csv files in `ratings` subdirectory of home directory,
e.g. `unzip -j netflix-prize-data.zip -d ~/ratings`

Result should resemble:
```
~/
|--> movie_titles.csv
|--> netflix-prize-data-kafka.jar
|--> scripts
  |--> 000-initialize-cluster.sh
  | ...
  |--> 004-input-generator.sh
|--> ratings
  |--> part-00.csv
  | ...
  |--> part-99.csv
```

Run `chmod +x ~/scripts/*`

## 4. Initialize cluster (technical terminal)

Run `~/scripts/000-initialize-cluster.sh`

## 5. Run consumer (consumer terminal)

Run `~/scripts/001-consumer.sh`

## 6. Run producer (producer terminal)

Run `~/scripts/002-producer.sh`

## 7. Run stream processing engine (streaming terminal)

Run `~/scripts/000-stream-engine.sh`

## 8. Run input generator (server terminal)

Run `~/scripts/004-input-generator.sh`

## 9. Have fun (technical terminal)

Connect to mysql:
```shell
mysql -h 127.0.0.1 -P 8080 -u root -ppassword
```

Select database:
```shell
USE netflix_prize_data;
```

It contains 2 tables:
- `movie_ratings` - ETL results
- `popular_movies` - anomaly detection results
