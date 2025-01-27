# Part 1: Spark Batch Processing

## Running this project
### 1. Setup environment using Docker Compose

```bash

cd spark-batch-processing

docker-compose up -d

```


### 2. Download [dataset](https://physionet.org/content/mimiciii/1.4/) and Extract the .csv files (ADMISSIONS.csv and PATIENTS.csv)

### 3. Copy .csv files in HDFS through the namenode docker container
```bash
# 1.copy csv files to docker container
docker cp /path/to/ADMISSIONS.csv namenode:/home/
docker cp /path/to/PATIENTS.csv namenode:/home/

# 2. copy files to HDFS
docker exec -it namenode bash
cd /home
hdfs dfs -mkdir /mimic-iii
hdfs dfs -put ADMISSIONS.csv /mimic-iii
hdfs dfs -put PATIENTS.csv /mimic-iii

```

### 3. Connect to jupyter server to run the notebooks and click on the link for jupyter lab
```bash
docker exec -it jupyter bash
jupyter server list
```
