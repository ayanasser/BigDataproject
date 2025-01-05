from kafka import KafkaProducer
import csv
import json
import time

# Kafka Configuration
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Path to the CSV file inside the Kafka container
file_path = '/admission.csv'  # Update this path if the file location changes

# Read and Publish Data
with open(file_path, 'r') as file:
    reader = csv.DictReader(file)
    for row in reader:
        # Prepare the record to match Flink schema
        record = {
            "HADM_ID": int(row["HADM_ID"]),
            "ADMITTIME": row["ADMITTIME"],  # String format as is
            "HOSPITAL_EXPIRE_FLAG": int(row["HOSPITAL_EXPIRE_FLAG"]), 
            "ADMISSION_TYPE": str(row["ADMISSION_TYPE"]).strip().upper(),
            "ADMISSION_LOCATION": str(row["ADMISSION_LOCATION"]).strip().upper(),
            "DIAGNOSIS": str(row["DIAGNOSIS"]).strip().upper(),
            "ETHNICITY": str(row["ETHNICITY"]).strip().upper(),
            "RELIGION": str(row["RELIGION"]).strip().upper(),
            "MARITAL_STATUS": str(row["MARITAL_STATUS"]).strip().upper()
        }

        # Send record to Kafka topic
        producer.send('patient-admissions', value=record)
        print(f"Sent: {record}")

        # Simulate real-time streaming delay
        time.sleep(1)

producer.flush()
producer.close()