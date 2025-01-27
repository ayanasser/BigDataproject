# Hadoop MIMIC-III Data Processing Pipeline

This project implements a data processing pipeline for the MIMIC-III healthcare dataset using Hadoop ecosystem tools. It includes data preprocessing, HDFS storage, Hive querying, and MapReduce analysis capabilities.

## Prerequisites

- Docker
- Python 3.7+
- Hadoop 3.x
- Apache Hive
- MIMIC-III Dataset access credentials

### Docker Setup

1. Install Docker following the [official Docker installation guide](https://docs.docker.com/get-docker/)
2. Verify installation:
   ```bash
   docker --version
   ```
 
### Python Dependencies

```bash
pip install pandas pyarrow subprocess.run datetime
```


## Environment Setup

### 1. Hadoop Environment Setup

1. Pull the Hadoop Docker image:
   ```bash
   docker image pull twkdocker/nuf23461-hadoop:latest
   ```
2. Create and access container:
   ```bash
   docker container run -it --name <container_name> twkdocker/nuf23461-hadoop:latest
   docker exec -it <container_name> bash
   ```


# docker-hive

This is a docker container for Apache Hive 2.3.2. It is based on https://github.com/big-data-europe/docker-hadoop so check there for Hadoop configurations.
This deploys Hive and starts a hiveserver2 on port 10000.
Metastore is running with a connection to postgresql database.
The hive configuration is performed with HIVE_SITE_CONF_ variables (see hadoop-hive.env for an example).

To run Hive with postgresql metastore:
```
    docker-compose up -d
```

To deploy in Docker Swarm:
```
    docker stack deploy -c docker-compose.yml hive
```

To run a PrestoDB 0.181 with Hive connector:

```
  docker-compose up -d presto-coordinator
```

This deploys a Presto server listens on port `8080`

## Testing
Load data into Hive:
```
  $ docker-compose exec hive-server bash
  # /opt/hive/bin/beeline -u jdbc:hive2://localhost:10000
  > CREATE TABLE pokes (foo INT, bar STRING);
  > LOAD DATA LOCAL INPATH '/opt/hive/examples/files/kv1.txt' OVERWRITE INTO TABLE pokes;
```

Then query it from PrestoDB. You can get [presto.jar](https://prestosql.io/docs/current/installation/cli.html) from PrestoDB website:
```
  $ wget https://repo1.maven.org/maven2/io/prestosql/presto-cli/308/presto-cli-308-executable.jar
  $ mv presto-cli-308-executable.jar presto.jar
  $ chmod +x presto.jar
  $ ./presto.jar --server localhost:8080 --catalog hive --schema default
  presto> select * from pokes;
```

## Project Structure

```
.
├── MIMIC_III_Dataset/      # Original MIMIC-III CSV files
├── MIMIC_III_Parquet/      # Preprocessed Parquet files
├── MIMIC_III_CSV/          # Preprocessed CSV files
├── mapreduce/
│   ├── mapper_age.py
│   ├── mapper_los.py
│   ├── mapper_readmissions.py
│   ├── reducer_age.py
│   ├── reducer_los.py
│   └── reducer_readmissions.py
└── main.py                 # Main processing script
```

## Data Pipeline Steps

### 1. Data Preprocessing

The pipeline preprocesses MIMIC-III CSV files and converts them to Parquet format:

- PATIENTS: Calculates patient ages
- ADMISSIONS: Computes length of stay
- DIAGNOSES_ICD: Standardizes ICD9 codes
- ICUSTAYS: Calculates ICU length of stay

```bash
python main.py
```

### 2. HDFS Data Upload

Upload preprocessed files to HDFS:

```bash
# Copy files to container
docker cp MIMIC_III_Parquet/PATIENTS.parquet container_name:/usr/local/
docker cp MIMIC_III_Parquet/ADMISSIONS.parquet container_name:/usr/local/
docker cp MIMIC_III_Parquet/DIAGNOSES_ICD.parquet container_name:/usr/local/
docker cp MIMIC_III_Parquet/ICUSTAYS.parquet container_name:/usr/local/

# Upload to HDFS
docker exec container_name hdfs dfs -put /usr/local/*.parquet /mimic-iii/
```

### 3. Hive Table Creation

The pipeline creates Hive tables for:
- Patients demographics
- Hospital admissions
- ICU stays
- Diagnosis codes

### 4. MapReduce Analysis

Run MapReduce jobs for specific analyses:

```bash
# Age Distribution Analysis
hadoop jar $HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-*.jar \
    -files mapper_age.py,reducer_age.py \
    -mapper "python mapper_age.py" \
    -reducer "python reducer_age.py" \
    -input /mimic-iii/PATIENTS.parquet \
    -output /output/age_distribution

# Length of Stay Analysis
hadoop jar $HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-*.jar \
    -files mapper_los.py,reducer_los.py \
    -mapper "python mapper_los.py" \
    -reducer "python reducer_los.py" \
    -input /mimic-iii/ADMISSIONS.parquet \
    -output /output/los_analysis
```

## HiveQL Queries

Example queries included in the pipeline:

1. Average Length of Stay per Diagnosis
2. ICU Readmission Distribution
3. Mortality Rates by Demographic Groups

Run queries using:
```bash
hive -f queries/analysis.hql
```

## Monitoring and Maintenance

1. Monitor Hadoop cluster:
```bash
# Check HDFS status
hdfs dfsadmin -report

# View running applications
yarn application -list
```

2. Monitor Hive:
```bash
# Check Hive metastore status
hive --service metastore &
```

## Troubleshooting

Common issues and solutions:

1. HDFS Connection Issues:
```bash
# Check if NameNode is running
docker ps | grep namenode

# Verify HDFS ports
netstat -tuln | grep 9000
```

2. Hive Connection Issues:
```bash
# Check Hive logs
docker logs hive-server

# Verify metastore connection
beeline -u jdbc:hive2://localhost:10000
```

## Contributing

1. Fork the repository
2. Create a feature branch
3. Submit a pull request
