
from kafka import KafkaConsumer
from prometheus_client import start_http_server, Gauge
import json
import threading

# Kafka Configuration
KAFKA_BOOTSTRAP_SERVERS = 'localhost:9092'
TOPIC_ADMISSION_TRENDS = 'admission-trends'
TOPIC_READMISSION_ANOMALIES = 'readmission-anomalies'

# Prometheus Configuration
PROMETHEUS_PORT = 8000

# Prometheus Metrics
admissions_metric = Gauge('admissions', 'Number of admissions by type', ['admission_type', 'time_window'])
anomalies_metric = Gauge('anomalies', 'Flagged readmissions', ['hadm_id', 'time_window'])

# Start Prometheus HTTP server
start_http_server(PROMETHEUS_PORT)

def consume_admission_trends():
    consumer = KafkaConsumer(
        TOPIC_ADMISSION_TRENDS,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        group_id='admission-trends-group',
        auto_offset_reset='earliest'
    )
    print(f"Subscribed to topic: {TOPIC_ADMISSION_TRENDS}")

    for message in consumer:
        data = message.value
        admission_type = data['ADMISSION_TYPE']
        total_admissions = data['total_admissions']
        time_window = data['time_window']

        # Print the message for debugging
        print(f"Admission Trends Message: {data}")

        # Update Prometheus metric
        admissions_metric.labels(admission_type=admission_type, time_window=time_window).set(total_admissions)

def consume_readmission_anomalies():
    consumer = KafkaConsumer(
        TOPIC_READMISSION_ANOMALIES,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        group_id='readmission-anomalies-group',
        auto_offset_reset='earliest'
    )
    print(f"Subscribed to topic: {TOPIC_READMISSION_ANOMALIES}")

    for message in consumer:
        data = message.value
        hadm_id = data['id']
        readmission_flag = data['readmission']
        time_window = data['admission_time']  # Using admission_time as the time window here

        # Print the message for debugging
        print(f"Readmission Anomalies Message: {data}")

        # Update Prometheus metric
        anomalies_metric.labels(hadm_id=hadm_id, time_window=time_window).set(readmission_flag)

# Run both consumers concurrently
if __name__ == "__main__":
    threading.Thread(target=consume_admission_trends, daemon=True).start()
    threading.Thread(target=consume_readmission_anomalies, daemon=True).start()

    print("Kafka Consumers running. Exposing metrics on port", PROMETHEUS_PORT)
    while True:
        pass


# from kafka import KafkaConsumer
# from prometheus_client import start_http_server, Gauge
# import json

# # Kafka Configuration
# KAFKA_TOPIC = 'admission-trends'
# KAFKA_BOOTSTRAP_SERVERS = 'localhost:9092'

# # Prometheus Configuration
# PROMETHEUS_PORT = 8000

# # Prometheus Metrics
# admissions_metric = Gauge('admissions', 'Number of admissions by type', ['admission_type'])

# # Start Prometheus HTTP server
# start_http_server(PROMETHEUS_PORT)

# consumer = KafkaConsumer(
#     KAFKA_TOPIC,
#     bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
#     value_deserializer=lambda m: json.loads(m.decode('utf-8')),
#     group_id='test-group',  # Ensure a group ID is set
#     auto_offset_reset='earliest'  # Start consuming from the earliest offset
# )
# print(f"Subscribed to topic: {KAFKA_TOPIC}")


# print(f"Kafka Consumer connected to topic '{KAFKA_TOPIC}' and exposing metrics on port {PROMETHEUS_PORT}")

# # Consume messages and update metrics
# for message in consumer:
#     print("Received")
#     data = message.value
#     admission_type = data['ADMISSION_TYPE']
#     total_admissions = data['total_admissions']

#     # Print the message for debugging
#     print(f"Received message: {data}")

#     # Update Prometheus metric
#     admissions_metric.labels(admission_type=admission_type).set(total_admissions)
