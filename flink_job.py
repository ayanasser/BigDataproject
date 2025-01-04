from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment
from pyflink.table import DataTypes, Schema
from pyflink.table.table_descriptor import TableDescriptor

# Initialize environment
env = StreamExecutionEnvironment.get_execution_environment()
table_env = StreamTableEnvironment.create(env)

# Connect to Kafka source for patient admissions
table_env.create_temporary_table(
    'Patients',
    TableDescriptor.for_connector('kafka')
        .schema(Schema.new_builder()
                .column('id', DataTypes.BIGINT())
                .column('admission_time', DataTypes.TIMESTAMP(3))
                .column('readmission', DataTypes.INT())
                .column_by_expression('rowtime', 'admission_time')
                .watermark('rowtime', "admission_time - INTERVAL '1' SECOND")
                .build())
        .option('properties.bootstrap.servers', 'kafka:9092')
        .option('topic', 'patient-admissions')
        .option('scan.startup.mode', 'earliest-offset')
        .format('json')
        .build()
)

# Task 1: Identifying trends in patient admissions over time
admission_trends = table_env.sql_query("""
    SELECT 
        TUMBLE_START(rowtime, INTERVAL '10' MINUTE) as time_window,
        COUNT(*) as total_admissions
    FROM Patients
    GROUP BY TUMBLE(rowtime, INTERVAL '10' MINUTE)
""")

# Write trends output to Kafka sink
table_env.create_temporary_table(
    'TrendsOutput',
    TableDescriptor.for_connector('kafka')
        .schema(Schema.new_builder()
                .column('time_window', DataTypes.TIMESTAMP(3))
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
    SELECT id, admission_time, readmission
    FROM Patients
    WHERE readmission > 0
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
        .option('topic', 'readmission-anomalies')
        .format('json')
        .build()
)

anomalies.execute_insert('AnomaliesOutput')

# Execute the Flink job
# env.execute("MIMIC-III Patient Admission Analysis")
