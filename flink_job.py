from pyflink.datastream import StreamExecutionEnvironment
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
                .column('ADMISSION_LOCATION', DataTypes.STRING())
                .column('DIAGNOSIS', DataTypes.STRING())
                .column('ETHNICITY', DataTypes.STRING())
                .column('RELIGION', DataTypes.STRING())
                .column('MARITAL_STATUS', DataTypes.STRING())
                .column_by_expression('rowtime', 'ADMITTIME')
                .watermark('rowtime', "ADMITTIME - INTERVAL '10' MINUTE")
                .build())
        .option('properties.bootstrap.servers', 'kafka:9092')
        .option('topic', 'patient-admissions')
        .option('scan.startup.mode', 'latest-offset')
        .format('json')
        .build()
)

# Task 1: Trends by admission type over time
admission_trends = table_env.sql_query("""
    SELECT 
        TUMBLE_START(rowtime, INTERVAL '10' MINUTE) as time_window,
        UPPER(TRIM(ADMISSION_TYPE)) as ADMISSION_TYPE,
        COUNT(*) as total_admissions
    FROM Patients
    GROUP BY TUMBLE(rowtime, INTERVAL '10' MINUTE), UPPER(TRIM(ADMISSION_TYPE))
""")

# Task 2: Trends by admission location
location_trends = table_env.sql_query("""
    SELECT 
        TUMBLE_START(rowtime, INTERVAL '10' MINUTE) as time_window,
        UPPER(TRIM(ADMISSION_LOCATION)) as ADMISSION_LOCATION,
        COUNT(*) as total_admissions
    FROM Patients
    GROUP BY TUMBLE(rowtime, INTERVAL '10' MINUTE), UPPER(TRIM(ADMISSION_LOCATION))
""")

# Task 3: Trends by diagnosis
diagnosis_trends = table_env.sql_query("""
    SELECT 
        TUMBLE_START(rowtime, INTERVAL '10' MINUTE) as time_window,
        UPPER(TRIM(DIAGNOSIS)) as DIAGNOSIS,
        COUNT(*) as total_admissions
    FROM Patients
    GROUP BY TUMBLE(rowtime, INTERVAL '10' MINUTE), UPPER(TRIM(DIAGNOSIS))
""")

# Write trends output to Kafka sinks
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

# Write location trends output
table_env.create_temporary_table(
    'LocationOutput',
    TableDescriptor.for_connector('kafka')
        .schema(Schema.new_builder()
                .column('time_window', DataTypes.TIMESTAMP(3))
                .column('ADMISSION_LOCATION', DataTypes.STRING())
                .column('total_admissions', DataTypes.BIGINT())
                .build())
        .option('properties.bootstrap.servers', 'kafka:9092')
        .option('topic', 'location-trends')
        .format('json')
        .build()
)

location_trends.execute_insert('LocationOutput')

# Write diagnosis trends output
table_env.create_temporary_table(
    'DiagnosisOutput',
    TableDescriptor.for_connector('kafka')
        .schema(Schema.new_builder()
                .column('time_window', DataTypes.TIMESTAMP(3))
                .column('DIAGNOSIS', DataTypes.STRING())
                .column('total_admissions', DataTypes.BIGINT())
                .build())
        .option('properties.bootstrap.servers', 'kafka:9092')
        .option('topic', 'diagnosis-trends')
        .format('json')
        .build()
)

diagnosis_trends.execute_insert('DiagnosisOutput')

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