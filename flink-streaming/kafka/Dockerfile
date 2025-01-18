# Use the Bitnami Kafka image as the base
FROM bitnami/kafka:latest

# Switch to root user for installations
USER root

# Install Python3, Pip, Kafka-Python library, and PostgreSQL client
RUN apt update && apt install -y python3 python3-pip wget && \
    ln -s /usr/bin/python3 /usr/bin/python && \
    pip3 install kafka-python psycopg2-binary --break-system-packages

# Download and install the PostgreSQL JDBC connector
RUN mkdir -p /opt/kafka/plugins/postgresql && \
    wget https://jdbc.postgresql.org/download/postgresql-42.6.0.jar -O /opt/kafka/plugins/postgresql/postgresql-42.6.0.jar

# Copy producer script and dataset into the container
COPY producer.py /producer.py
COPY ADMISSIONS.csv /admission.csv

# Set plugin.path in Kafka Connect configuration
ENV KAFKA_OPTS="-Djava.awt.headless=true"

# Switch back to the kafka user for security
USER 1001
