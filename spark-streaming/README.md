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
- **Prometheus**: For exposing Kafka consumer metrics.

### **Key Scripts**
- **Kafka Producer:** Reads ICU patient vitals from the MIMIC-III dataset and publishes them to a Kafka topic.
- **Spark Job:** Processes streaming data using SQL queries to:
  - Monitor vitals.
  - Generate real-time alerts for abnormal signs.
  - Aggregate statistics for visualization.
- **Kafka Consumer:** Consumes data from Kafka topics and exposes Prometheus metrics for monitoring.

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
   This will start Kafka, Zookeeper, Spark, PostgreSQL, Prometheus, and Grafana.

### **3. Mount Dataset for Kafka Producer**
Ensure the MIMIC-III dataset is accessible within the Kafka container by verifying the `volumes` configuration in `docker-compose.yml`.

### **4. Run Kafka Producer**
#### Execute the Kafka producer to simulate real-time data ingestion:
```bash
docker exec -it kafka python3 /app/producer.py
```
#### Verify data in Kafka topic:
```bash
docker exec -it kafka kafka-console-consumer.sh --topic patient-vitals --from-beginning --bootstrap-server localhost:9092
```

### **5. Create Kafka Topic for Abnormal Vital Signs**
Create a Kafka topic for processed data:
```bash
docker exec -it kafka kafka-topics.sh --create --topic abnormal-vital-sign --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
```

### **6. Run Spark Job**
Submit the Spark job from the Spark master container:
```bash
docker exec -it spark-master spark-submit \
--master spark://spark-master:7077 \
--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.2 \
--jars /opt/bitnami/spark/ivy/* /app/spark_job.py
```
#### Verify data written to the Kafka topic:
```bash
docker exec -it kafka kafka-console-consumer.sh --topic abnormal-vital-sign --from-beginning --bootstrap-server localhost:9092
```

### **7. Run Kafka Consumer for Prometheus Metrics**
To expose Kafka topic metrics to Prometheus:
1. Run the Kafka consumer script:
   ```bash
   docker exec -it kafka python3 /app/kafka_consumer.py
   ```
   Ensure the script is consuming data from `patient-vitals` and `abnormal-vital-sign` topics and exposing metrics on port 8000.
2. Verify Prometheus is scraping metrics:
   - Access Prometheus at `http://localhost:9090/targets` and confirm the target for `http://kafka:8000/metrics` is active.

---

## Accessing Services

### **1. Grafana Dashboard**
1. Access Grafana at `http://localhost:3000`.
2. Log in with default credentials:
   - **Username:** `admin`
   - **Password:** `admin`
3. Add Prometheus as a data source:
   - Navigate to **Configuration** > **Data Sources**.
   - Click **Add data source** and select **Prometheus**.
   - Enter the URL: `http://prometheus:9090`.
   - Click **Save & Test**.

4. Create a Dashboard:
   - Click **+** > **Dashboard** > **Add a new panel**.
   - Use PromQL queries to visualize metrics. Examples:
     - For patient vital signs:
       ```promql
       patient_vitals_value{vital_sign="220292"}
       ```
     - For abnormal vital sign alerts:
       ```promql
       abnormal_vital_sign_alert{alert="Heart Rate - Abnormal"}
       ```
   - Select the appropriate visualization (e.g., Gauge, Graph).
   - Save the dashboard with a meaningful name (e.g., "ICU Monitoring").

### **2. Prometheus Web Interface**
- Access Prometheus at `http://localhost:9090` to directly query and visualize metrics.

---

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
1. **Kafka Producer Not Working:** Ensure the dataset path is correctly mounted to the Kafka container.
2. **Spark Job Failing:** Check the Spark logs for errors and ensure the Spark master is reachable.
3. **Prometheus Not Scraping Metrics:** Verify the Kafka consumer is running and exposing metrics on the correct port.
4. **Grafana Not Showing Data:** Ensure the correct PromQL queries are used, and Prometheus is configured as a data source.

---

## Future Enhancements
- Add support for additional vital signs and alert conditions.
- Incorporate machine learning models for predictive analytics.
- Expand to multi-cluster deployments for high availability.

---

## License
This project is licensed under the MIT License.


