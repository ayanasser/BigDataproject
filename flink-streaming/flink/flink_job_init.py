from pyflink.datastream import StreamExecutionEnvironment
"""
A Flink streaming job that processes patient admission data from Kafka.

This script sets up a Flink streaming environment to analyze patient admission data
in real-time. It performs two main tasks:
1. Tracks admission trends by counting admissions in 10-minute windows
2. Detects potential anomalies in patient readmissions

The job reads from a Kafka topic 'patient-admissions' containing admission records
and writes results to two output topics:
- 'admission-trends': Contains admission counts per 10-minute window
- 'readmission-anomalies': Contains records of patients flagged for readmission

Attributes in input data:
    HADM_ID (bigint): Unique identifier for hospital admission
    ADMITTIME (timestamp): Time of admission
    HOSPITAL_EXPIRE_FLAG (int): Flag indicating readmission status

Dependencies:
    - Apache Flink
    - Apache Kafka
    - PyFlink

Configuration:
    - Kafka bootstrap servers: kafka:9092
    - Default parallelism: 1
    - Consumer group ID: flink-group
    - Offset reset: earliest
"""
from pyflink.table import StreamTableEnvironment
from pyflink.table import DataTypes, Schema
from pyflink.table.table_descriptor import TableDescriptor

# Initialize environment
env = StreamExecutionEnvironment.get_execution_environment()
env.set_parallelism(1)  # Set parallelism to 1 to minimize required slots
table_env = StreamTableEnvironment.create(env)



# Connect to Kafka source for patient admissions
table_env.create_temporary_table(
    'Patients',
    TableDescriptor.for_connector('kafka')
        .schema(Schema.new_builder()
                .column('HADM_ID', DataTypes.BIGINT())
                .column('ADMITTIME', DataTypes.TIMESTAMP(3))
                .column('HOSPITAL_EXPIRE_FLAG', DataTypes.INT())
                .column('ADMISSION_TYPE', DataTypes.STRING())
                .column_by_expression('rowtime', 'ADMITTIME')
                .watermark('rowtime', "ADMITTIME - INTERVAL '1' MINUTE")
                .build())
        .option('properties.bootstrap.servers', 'kafka:9092')
        .option('topic', 'patient-admissions')
        .option('properties.group.id', 'flink-group')  # Group ID for Kafka consumer
        .option('properties.auto.offset.reset', 'earliest')  # Reset offsets if needed
        .option('scan.startup.mode', 'earliest-offset')
        .format('json')
        .build()
)

# Task 1: Identifying trends in patient admissions over time
admission_trends = table_env.sql_query("""
    SELECT 
        TUMBLE_START(rowtime, INTERVAL '1' MINUTE) as time_window,
        UPPER(TRIM(ADMISSION_TYPE)) as ADMISSION_TYPE,
        COUNT(*) as total_admissions
    FROM Patients
    GROUP BY TUMBLE(rowtime, INTERVAL '1' MINUTE), ADMISSION_TYPE

""")


# Write trends output to Kafka sink
table_env.create_temporary_table(
    'TrendsOutput',
    TableDescriptor.for_connector('kafka')
        .schema(Schema.new_builder()
                .column('time_window', DataTypes.TIMESTAMP(3))
                .column('ADMISSION_TYPE', DataTypes.STRING())
                .column('total_admissions', DataTypes.BIGINT())
                .build())
        .option('properties.bootstrap.servers', 'kafka:9092')
        .option('topic', 'admission-trends')
        .format('json')
        .build()
)

admission_trends.execute_insert('TrendsOutput')

# Task 2: Detecting anomalies in patient readmission patterns
anomalies = table_env.sql_query("""
    SELECT HADM_ID as id, ADMITTIME as admission_time, HOSPITAL_EXPIRE_FLAG as readmission
    FROM Patients
    WHERE HOSPITAL_EXPIRE_FLAG > 0
""")

# Write anomalies output to Kafka sink
table_env.create_temporary_table(
    'AnomaliesOutput',
    TableDescriptor.for_connector('kafka')
        .schema(Schema.new_builder()
                .column('id', DataTypes.BIGINT())
                .column('admission_time', DataTypes.TIMESTAMP(3))
                .column('readmission', DataTypes.INT())
                .build())
        .option('properties.bootstrap.servers', 'kafka:9092')
        .option('scan.startup.mode', 'earliest-offset')
        .option('topic', 'readmission-anomalies')
        .format('json')
        .build()
)

anomalies.execute_insert('AnomaliesOutput')