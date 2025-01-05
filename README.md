# Steps to Build Flink Environment

## 1. Pull Flink Docker Image
Download the official Apache Flink image from Docker Hub:

           $ docker pull flink:1.15.2-scala_2.12 


## 2. Start Flink Cluster in Docker
Single Node Cluster:

Run Flink JobManager:

       $ docker network create flink-network
       $ docker run --name jobmanager --network flink-network -p 8081:8081 -d flink:1.15.2-scala_2.12 jobmanager


Start Flink TaskManager:

    $ docker run --name taskmanager --network flink-network -d flink:1.15.2-scala_2.12 taskmanager

Access the Flink Web UI:

    Open your browser and go to:
    http://localhost:8081


## 3. Connect Flink to Streaming Source
Using Kafka in Docker:
Start Kafka and Zookeeper Containers:


    $ docker run -d --name zookeeper --network flink-network -e ALLOW_ANONYMOUS_LOGIN=yes -p 2181:2181 bitnami/zookeeper:latest
    $ docker run -d --name kafka --network flink-network -e KAFKA_CFG_ZOOKEEPER_CONNECT=zookeeper:2181 -e KAFKA_CFG_AUTO_CREATE_TOPICS_ENABLE=true -p 9092:9092 bitnami/kafka:latest

## 5.Create Kafka Topic:


    $ docker exec -it kafka kafka-topics.sh --create --topic patient-admissions --bootstrap-server kafka:9092 --partitions 1 --replication-factor 1

# setup python inside Flink Env

    $ docker exec -it jobmanager bash
    $ apt update
    $ apt install python3
    $ ln -s /usr/bin/python3 /usr/bin/python
    $ apt install python3-pip
    $ pip3 install apache-flink


# Tip: 
    IF you have issue running the below code with the flink docker, restart the jobmanager and  taskmanager after 
    setup the python env 

        $ docker restart jobmanager taskmanager

# Run the Job:
 Copy the job to the container:

    $ docker cp flink_job.py jobmanager:/opt/flink-job.py

Submit the job:

    $ docker exec -it jobmanager flink run -py /opt/flink-job.py


# Errors regarding Kafka inside flink:

    1- Missing Kafka connectors in flink:
  solution:
    - Download the Kafka connector jar file from the official Apache Flink website.
    - Copy the jar file to the /opt/flink/lib folder in the Flink container.
    - Restart the Flink cluster.


        $ wget https://repo1.maven.org/maven2/org/apache/flink/flink-connector-kafka/1.15.0/flink-connector-kafka-1.15.0.jar
        $ docker cp flink-connector-kafka-1.15.0.jar jobmanager:/opt/flink/lib/
        $ docker cp flink-connector-kafka-1.15.0.jar taskmanager:/opt/flink/lib/
        $ docker restart jobmanager taskmanager


    2- Missing Kafka Client Dependency:
    solution:
        - Download the Kafka client jar file from the official Apache Kafka website.    
        $ wget https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.2.0/kafka-clients-3.2.0.jar
        $ docker cp kafka-clients-3.2.0.jar jobmanager:/opt/flink/lib/
        $ docker cp kafka-clients-3.2.0.jar taskmanager:/opt/flink/lib/
        $ docker restart jobmanager taskmanager    
    
    
    3- Kafka broker not reachable:
    solution:
    - Check the Kafka broker IP address and port number.
    - Check the Kafka broker container status.
    - Check the Kafka broker logs for any errors.




# Kafka producer:

    ## setup python inside Kafka:
        $ docker exec -it --user root kafka bash
        $ apt update
        $ apt install python3
        $ ln -s /usr/bin/python3 /usr/bin/python
        $ apt install python3-pip
        $ pip3 install kafka-python or pip install kafka-python --break-system-packages


    ## check kafka topics:
    
        $ docker exec -it kafka kafka-topics.sh --list --bootstrap-server localhost:9092


    ## run the producer:
      $ docker cp ADMISSIONS.csv kafka:/admission.csv
      $ docker cp producer.py kafka:/producer.py
      $ docker exec -it kafka python3 /producer.py
        # verify data in kafka topic
        $ docker exec -it kafka kafka-console-consumer.sh --topic patient-admissions --from-beginning --bootstrap-server localhost:9092


    # to check the output:

    1. check the dashboard for the running processes
    2. check the output in the kafka consumer:
        $ docker exec -it kafka kafka-console-consumer.sh --topic admission-trends --bootstrap-server localhost:9092 --from-beginning
        $ docker exec -it kafka kafka-console-consumer.sh --topic patient-admissions --bootstrap-server localhost:9092 --from-beginning


# Monitoring and Results
Flink Dashboard:

Go to http://localhost:8081 to monitor job execution, performance, and logs.

## Kafka Consumer:

Check the output:

     $ docker exec -it kafka kafka-console-consumer.sh --topic patient-admissions --bootstrap-server localhost:9092 --from-beginning


# Metrics to Compare with spark:

Check dashboards at:
Flink: http://localhost:8081
Spark: http://localhost:8080


# Next : Dashboarding and visualization

The results are pushed to a Kafka topic (admission-trends) for real-time visualization using dashboards like Grafana or Tableau.


    
# Additional info during flink journey:

# get the running jobs:
    docker exec -it jobmanager flink list

# to stop the job:
    docker exec -it jobmanager flink cancel <job_id>
    or from the flink dashboard

## to run flink SQL client:

    $ docker exec -it jobmanager ./bin/sql-client.sh

## to set more slots:
./bin/taskmanager.sh start 

# to check kafka topics
    $ docker exec -it --user root kafka bash
    $ docker exec -it kafka kafka-topics.sh --list --bootstrap-server localhost:9092
    or 
    $ kafka-topics.sh --bootstrap-server localhost:9092 --list


# Grafana and prometheus

http://localhost:9090/query

# Next enhanceents:
    - make alerts and threshold with grafana, by setting up SMTP server protocool
 - add database to store the data
