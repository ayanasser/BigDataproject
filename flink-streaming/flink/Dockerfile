# Use the official Flink image as the base
FROM flink:1.15.2-scala_2.12

# Switch to root user for installations
USER root

# Install Python3, Pip, and Apache Flink Python API
RUN apt update && apt install -y python3 python3-pip wget && \
    ln -s /usr/bin/python3 /usr/bin/python && \
    pip3 install apache-flink

# Download and add Flink Kafka Connector JAR
RUN wget -P /opt/flink/lib/ https://repo1.maven.org/maven2/org/apache/flink/flink-connector-kafka/1.15.0/flink-connector-kafka-1.15.0.jar
RUN wget  -P /opt/flink/lib/ https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.2.0/kafka-clients-3.2.0.jar
COPY flink_job.py /opt/flink_job.py
# Switch back to flink user
USER flink
