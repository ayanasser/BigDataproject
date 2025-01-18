from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, struct, to_json, when
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, TimestampType

# Define schema for Kafka messages
schema = StructType([
    StructField("subject_id", IntegerType(), True),
    StructField("icustay_id", IntegerType(), True),
    StructField("charttime", TimestampType(), True),
    StructField("vital_sign", IntegerType(), True),  # Assuming it's integer in Kafka
    StructField("value", FloatType(), True),
    StructField("unit", StringType(), True),
])
print("start spark job.......................................................")
# Initialize Spark session
spark = SparkSession.builder \
    .appName("ICU_Vital_Streaming") \
    .config("spark.sql.streaming.checkpointLocation", "/tmp/checkpoint") \
    .getOrCreate()

# Read data from Kafka
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "patient-vitals") \
    .option("startingOffsets", "latest") \
    .load()
print("data read from kafka.....................................................")
# Deserialize Kafka value field (JSON)
parsed_df = df.selectExpr("CAST(value AS STRING) as json") \
    .select(from_json(col("json"), schema).alias("data")) \
    .select("data.*")

# Identify abnormal vitals
abnormal_vitals = parsed_df.withColumn(
    "alert",
    when((col("vital_sign") == 211) & ((col("value") < 60) | (col("value") > 100)), "Abnormal Heart Rate")
    .when((col("vital_sign") == 618) & (col("value") < 90), "Low SpO2")
    .when((col("vital_sign") == 223762) & ((col("value") < 36) | (col("value") > 38)), "Abnormal Temperature")
    .otherwise(None)
).filter("alert IS NOT NULL")

print("abnormal vitals identified.....................................................")

# Write abnormal vitals to Kafka
kafka_output = abnormal_vitals.select(to_json(struct("*")).alias("value"))
query = kafka_output.writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("topic", "abnormal-vital-sign") \
    .option("checkpointLocation", "/tmp/checkpoint_kafka") \
    .start()
print("abnormal vitals written to kafka.....................................................")
query.awaitTermination()
