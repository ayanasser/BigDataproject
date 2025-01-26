from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, struct, to_json, when, broadcast, concat_ws, lit
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, TimestampType

def load_d_items(spark):
    # Load D_ITEMS.csv
    d_items_schema = StructType([
        StructField("ROW_ID", IntegerType(), True),
        StructField("ITEMID", IntegerType(), True),
        StructField("LABEL", StringType(), True),
        StructField("ABBREVIATION", StringType(), True),
        StructField("DBSOURCE", StringType(), True),
        StructField("LINKSTO", StringType(), True),
        StructField("CATEGORY", StringType(), True),
        StructField("UNITNAME", StringType(), True),
        StructField("PARAM_TYPE", StringType(), True),
        StructField("CONCEPTID", IntegerType(), True)
    ])
    
    return spark.read \
        .schema(d_items_schema) \
        .option("header", "true") \
        .csv("/app/D_ITEMS.csv")

# Define schema for Kafka messages
schema = StructType([
    StructField("subject_id", IntegerType(), True),
    StructField("icustay_id", IntegerType(), True),
    StructField("charttime", TimestampType(), True),
    StructField("vital_sign", IntegerType(), True),
    StructField("value", FloatType(), True),
    StructField("unit", StringType(), True),
])

print("start spark job.......................................................")

# Initialize Spark session
spark = SparkSession.builder \
    .appName("ICU_Vital_Streaming") \
    .config("spark.sql.streaming.checkpointLocation", "/tmp/checkpoint") \
    .config("spark.executor.memory", "2g") \
    .config("spark.driver.memory", "2g") \
    .config("spark.executor.cores", "2") \
    .config("spark.driver.cores", "2") \
    .config("spark.default.parallelism", "4") \
    .config("spark.streaming.kafka.maxRatePerPartition", "100") \
    .config("spark.sql.shuffle.partitions", "4") \
    .master("local[*]") \
    .getOrCreate()

# Set log level to reduce noise
spark.sparkContext.setLogLevel("WARN")

print("Spark session created.....................................................")

try:
    # Load D_ITEMS as a broadcast variable
    d_items_df = broadcast(load_d_items(spark))
    print("D_ITEMS loaded and broadcasted.................................................")

    # Read data from Kafka
    df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:9092") \
        .option("subscribe", "patient-vitals") \
        .option("startingOffsets", "latest") \
        .option("failOnDataLoss", "false") \
        .load()
    
    print("Kafka stream initialized.....................................................")
    
    # Deserialize Kafka value field (JSON)
    parsed_df = df.selectExpr("CAST(value AS STRING) as json") \
        .select(from_json(col("json"), schema).alias("data")) \
        .select("data.*")
    

    # Filter for selected ITEMIDs (HR, SpO2, Temperature)
    filtered_df = parsed_df.filter(
        col("vital_sign").isin(220045, 220277, 223762)  # Replace with actual ITEMIDs
    )


    # Join with D_ITEMS
    enriched_df = filtered_df.join(
        d_items_df,
        parsed_df.vital_sign == d_items_df.ITEMID,
        "left"
    )
    
    print("Stream processing started.....................................................")
    
    # Identify abnormal vitals with enriched data using ABBREVIATION
    abnormal_vitals = enriched_df.withColumn(
        "alert",
        when((col("ABBREVIATION") == "HR") & ((col("value") < 60) | (col("value") > 100)), # HR 220045
             concat_ws(" - ", col("LABEL"), lit("Abnormal")))    
        .when((col("ABBREVIATION") == "SpO2") & (col("value") < 90), 
              concat_ws(" - ", col("LABEL"), lit("Low")))
        .when((col("ABBREVIATION") == "Temperature C") & ((col("value") < 36) | (col("value") > 38)), 
              concat_ws(" - ", col("LABEL"), lit("Abnormal")))
        .otherwise(None)
    ).filter("alert IS NOT NULL")

    # Convert all fields to string format and create a JSON string
    kafka_output = abnormal_vitals.select(
        to_json(struct(
            col("subject_id").cast("string").alias("subject_id"),
            col("icustay_id").cast("string").alias("icustay_id"),
            col("charttime").cast("string").alias("charttime"),
            col("vital_sign").cast("string").alias("vital_sign"),
            col("ABBREVIATION").alias("vital_sign_abbrev"),
            col("LABEL").alias("vital_sign_label"),
            col("CATEGORY").alias("category"),
            col("value").cast("string").alias("value"),
            col("UNITNAME").alias("unit"),
            col("alert").alias("alert")
        )).alias("value")
    )

    print("abnormal vitals identified.....................................................")

    # Write abnormal vitals to Kafka
    query = kafka_output.writeStream \
        .outputMode("append") \
        .format("kafka") \
        .trigger(processingTime='5 seconds') \
        .option("kafka.bootstrap.servers", "kafka:9092") \
        .option("topic", "abnormal-vital-sign") \
        .option("checkpointLocation", "/tmp/checkpoint_kafka") \
        .start()
    
    print("abnormal vitals written to kafka.....................................................")
    query.awaitTermination()

except Exception as e:
    print(f"Error occurred: {str(e)}")
    spark.stop()