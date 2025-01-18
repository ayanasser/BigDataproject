from kafka import KafkaConsumer
import psycopg2
import json

# Kafka Configuration
KAFKA_BROKER = 'localhost:9092'

# PostgreSQL Configuration
PG_HOST = 'postgres'
PG_PORT = 5432
PG_USER = 'admin'
PG_PASSWORD = 'password'

# Database Mapping for Each Topic
TOPIC_DB_MAPPING = {
    "admission-trends": {
        "database": "admission_db",
        "insert_query": """
            INSERT INTO admission_trends (time_window, admission_type, total_admissions)
            VALUES (%s, %s, %s)
        """,
    },
    "location-trends": {
        "database": "location_db",
        "insert_query": """
            INSERT INTO location_trends (time_window, admission_location, total_admissions)
            VALUES (%s, %s, %s)
        """,
    },
    "diagnosis-trends": {
        "database": "diagnosis_db",
        "insert_query": """
            INSERT INTO diagnosis_trends (time_window, diagnosis, total_admissions)
            VALUES (%s, %s, %s)
        """,
    },
    "readmission-anomalies": {
        "database": "anomalies_db",
        "insert_query": """
            INSERT INTO anomalies (id, admission_time, readmission)
            VALUES (%s, %s, %s)
        """,
    },
}

# Initialize Kafka Consumers for Each Topic
consumers = {}
for topic in TOPIC_DB_MAPPING:
    consumers[topic] = KafkaConsumer(
        topic,
        bootstrap_servers=[KAFKA_BROKER],
        auto_offset_reset="earliest",
        value_deserializer=lambda x: json.loads(x.decode("utf-8")),
    )

# Process Each Topic
for topic, consumer in consumers.items():
    db_config = TOPIC_DB_MAPPING[topic]
    database_name = db_config["database"]
    insert_query = db_config["insert_query"]

    print(f"Processing topic: {topic}, database: {database_name}")

    # Connect to the respective PostgreSQL database
    conn = psycopg2.connect(
        host=PG_HOST,
        port=PG_PORT,
        user=PG_USER,
        password=PG_PASSWORD,
        dbname=database_name,
    )
    cursor = conn.cursor()

    for message in consumer:
        record = message.value
        print(f"Received message from {topic}: {record}")

        # Map the record fields based on the topic
        try:
            if topic == "admission-trends":
                cursor.execute(
                    insert_query,
                    (
                        record["time_window"],
                        record["ADMISSION_TYPE"],
                        int(record["total_admissions"]),
                    ),
                )
            elif topic == "location-trends":
                cursor.execute(
                    insert_query,
                    (
                        record["time_window"],
                        record["ADMISSION_LOCATION"],
                        int(record["total_admissions"]),
                    ),
                )
            elif topic == "diagnosis-trends":
                cursor.execute(
                    insert_query,
                    (
                        record["time_window"],
                        record["DIAGNOSIS"],
                        int(record["total_admissions"]),
                    ),
                )
            elif topic == "readmission-anomalies":
                cursor.execute(
                    insert_query,
                    (
                        int(record["id"]),
                        record["admission_time"],
                        int(record["readmission"]),
                    ),
                )
            # Commit the transaction
            conn.commit()
        except Exception as e:
            print(f"Error processing record from {topic}: {e}")

    # Close the connection after processing
    cursor.close()
    conn.close()
