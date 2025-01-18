# Real-Time ICU Patient Monitoring System

This project is designed to simulate and process real-time streams of ICU patient vitals from the MIMIC-III dataset. The system utilizes Apache Kafka for data ingestion, Apache Spark for processing, PostgreSQL for data storage, and Grafana for visualization.

---

## Features

- **Real-Time Data Ingestion:** Simulates streaming data ingestion from the MIMIC-III dataset using Kafka.
- **Stream Processing:** Processes ICU patient vitals in real-time using Spark Streaming with SQL queries.
- **Alerts:** Generates real-time alerts for abnormal vital signs (e.g., heart rate, SpO2, temperature).
- **Dashboards:** Aggregates data for near-real-time dashboards in Grafana.

---

## Project Components

### **Dockerized Services**
- **Kafka**: For real-time data ingestion.
- **Spark**: For real-time stream processing.
- **PostgreSQL**: For storing processed data.
- **Grafana**: For visualizing alerts and aggregated statistics.

### **Key Scripts**
- **Kafka Producer:** Reads ICU patient vitals from the MIMIC-III dataset and publishes them to a Kafka topic.
- **Spark Job:** Processes streaming data using SQL queries to:
  - Monitor vitals.
  - Generate real-time alerts for abnormal signs.
  - Aggregate statistics for visualization.

---

## Prerequisites

- Docker and Docker Compose installed on your system.
- MIMIC-III dataset files available locally.

---

## Setup Instructions

### **1. Clone the Repository**
```bash
git clone https://github.com/ayanasser/BigDataproject.git
cd BigDataproject
```

### **2. Start Dockerized Services**
1. Ensure the MIMIC-III dataset is placed in the project directory.
2. Run the following command:
   ```bash
   docker-compose up --build -d
   ```
   This will start Kafka, Zookeeper, Spark, PostgreSQL, and Grafana.

### **3. Mount Dataset for Kafka Producer**
Ensure the MIMIC-III dataset is accessible within the Kafka container by verifying the `volumes` configuration in `docker-compose.yml`.

### **4. Run Kafka Producer**

#### Execute the Kafka producer to simulate real-time data ingestion:
```bash
docker exec -it kafka python3 /app/producer.py
```

#### verify data in kafka topic
    $ docker exec -it kafka kafka-console-consumer.sh --topic patient-vitals --from-beginning --bootstrap-server localhost:9092

### **5. Run Spark Job**
Submit the Spark job from the Spark master container:
```bash
docker exec -it spark-master spark-submit 
--master spark://spark-master:7077 
--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.2 
--jars /opt/bitnami/spark/ivy/* /app/spark_job.py;
```

---

## Accessing Services

### **1. Kafka UI (Optional)**
- Use a Kafka UI tool (e.g., Kafka UI or Conduktor) to monitor Kafka topics.
- Example UI access: `http://localhost:8080` (if Kafka UI is configured).

<!-- ### **2. Grafana Dashboard**
- Access Grafana at `http://localhost:3000`.
- Default credentials:
  - **Username:** admin
  - **Password:** admin
- Create dashboards using data from PostgreSQL.

### **3. PostgreSQL**
- Connect to PostgreSQL to query raw and processed data:
  ```bash
  docker exec -it postgres psql -U <username> -d <database_name>
  ```

--- -->
<!-- 
## Customization

### **Environment Variables**
Adjust the `.env` file to configure:
- Kafka topics.
- Spark master and worker settings.
- PostgreSQL credentials.

### **SQL Queries**
Modify the Spark SQL queries in the `spark_job.py` script to adapt monitoring and aggregation logic to your requirements.

---

## Troubleshooting

### **Common Issues**
1. **Kafka Producer Not Working**: Ensure the dataset path is correctly mounted to the Kafka container.
2. **Spark Job Failing**: Check the Spark logs for errors and ensure the Spark master is reachable.
3. **Grafana Not Showing Data**: Verify that PostgreSQL tables are populated and the correct data source is configured in Grafana.

---

## Future Enhancements
- Add support for additional vital signs and alert conditions.
- Incorporate machine learning models for predictive analytics.
- Expand to multi-cluster deployments for high availability.

---

## License
This project is licensed under the MIT License.

---

Feel free to reach out for any assistance or collaboration opportunities!
 -->
