from kafka import KafkaProducer
import csv
import json
import time

# Kafka Producer Configuration
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Read CSV file and send rows to Kafka
with open('admission.csv', 'r') as file:
    reader = csv.DictReader(file)
    for row in reader:
        # Transform data to match Flink schema
        record = {
            "id": int(row["id"]),
            "admission_time": row["admission_time"],
            "readmission": int(row["readmission"])
        }
        producer.send('patient-admissions', value=record)
        print(f"Sent: {record}")
        time.sleep(1)  # Simulate streaming by adding delay

producer.flush()
producer.close()
