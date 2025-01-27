import pandas as pd
import subprocess
from datetime import datetime
import pyarrow.parquet as pq
import pyarrow as pa

# ==================================================
# Step 1: Upload MIMIC-III Dataset to HDFS
# ==================================================

def copy_parquet_files_to_container():
    container_path = "container_name:/usr/local/"
    # List of MIMIC-III CSV files to upload
    mimic_csv_files = ["MIMIC_III_Parquet/PATIENTS.parquet", "MIMIC_III_Parquet/ADMISSIONS.parquet", "MIMIC_III_Parquet/DIAGNOSES_ICD.parquet", "MIMIC_III_Parquet/ICUSTAYS.parquet"]

    # Upload each file to HDFS inside the container
    for file in mimic_csv_files:
        subprocess.run(["docker", "cp", file, container_path])
        print(f"file {file} uploaded to container home.")

def copy_csv_files_to_container():
    container_path = "container_name:/usr/local/"
    # List of MIMIC-III CSV files to upload
    
    #hdfs dfs -put -f ICUSTAYS_processed.csv /mimic_csv
    
    mimic_csv_files = ["MIMIC_III_CSV/PATIENTS_processed.csv", "MIMIC_III_CSV/ADMISSIONS_processed.csv", "MIMIC_III_CSV/DIAGNOSES_ICD_processed.csv", "MIMIC_III_CSV/ICUSTAYS_processed.csv"]

    # Upload each file to HDFS inside the container
    for file in mimic_csv_files:
        subprocess.run(["docker", "cp", file, container_path])
        print(f"file {file} uploaded to container home.")
        
def copy_Map_Red_files_to_container():
    container_path = "container_name:/usr/local/"
    # List of MIMIC-III CSV files to upload
    
    #hdfs dfs -put -f ICUSTAYS_processed.csv /mimic_csv
    
    mimic_csv_files = ["mapper_age.py", "mapper_los.py", "mapper_readmissions.py", "reducer_age.py" , "reducer_los.py", "reducer_readmissions.py"]

    # Upload each file to HDFS inside the container
    for file in mimic_csv_files:
        subprocess.run(["docker", "cp", file, container_path])
        print(f"file {file} uploaded to container home.")
        
def upload_to_hdfs_from_container_home():
    """
    Uploads all MIMIC-III Parquet files to HDFS.
    """
    # Docker container path 
    container_name = "container_name"
    #hdfs path
    hdfs_path = "/mimic-iii"
    #hdfs dfs -copyFromLocal <path-of-data-file> <hdfs-path>
    #hdfs dfs -put -f PATIENTS.parquet /mimic-iii
    #hdfs dfs -put -f PATIENTS.parquet /mimic-iii
    """
    Uploads all MIMIC-III CSV files to HDFS.
    """
    # Create the HDFS directory inside the container
    subprocess.run(["docker", "exec" , container_name, "bash"]) 

    # List of MIMIC-III CSV files to upload
    mimic_csv_files = ["PATIENTS.parquet", "ADMISSIONS.parquet", "DIAGNOSES_ICD.parquet", "ICUSTAYS.parquet"]

    # Upload each file to HDFS inside the container
    for file in mimic_csv_files:
        subprocess.run(["hdfs", "dfs", "-put", file, hdfs_path])
        print(f" file{file} from MIMIC-III parquet uploaded to HDFS.")

# ==================================================
# Step 2: Preprocess CSV Files and Save as Parquet
# ==================================================
def calculate_age(df):
    df['DOB'] = pd.to_datetime(df['DOB'])
    df['DOD'] = pd.to_datetime(df['DOD'])
    
    # Using timedelta64[Y] for years
    df['age'] = ((df['DOD'] - df['DOB']).dt.total_seconds()/(365.25*24*60*60)).astype(int)
    return df

# Alternative method using dateutil
from dateutil.relativedelta import relativedelta

def preprocess_patients():
    """
    Preprocesses the PATIENTS.csv file and saves it as a Parquet file.
    """
    # Load the PATIENTS.csv file
    patients_df = pd.read_csv("MIMIC_III_Dataset/PATIENTS.csv")
    patients_df = patients_df.dropna()
    
    # Convert DOB and DOD columns to datetime
    patients_df['DOB'] = pd.to_datetime(patients_df['DOB'])
    patients_df['DOD'] = pd.to_datetime(patients_df['DOD'], errors='coerce')

    def calculate_age(row):
        dob = row['DOB']
        dod = row['DOD'] if pd.notna(row['DOD']) else pd.Timestamp.now()
        age = dod.year - dob.year
        if (dod.month, dod.day) < (dob.month, dob.day):
            age -= 1
        return age

    patients_df['AGE'] = patients_df.apply(calculate_age, axis=1)
    # Save the preprocessed file as Parquet
    patients_df.to_parquet("MIMIC_III_Parquet/PATIENTS.parquet", index=False)
    patients_df.to_csv("MIMIC_III_CSV/PATIENTS_processed.csv", index=False)
    print("PATIENTS.csv preprocessed and saved as csv and Parquet.")

def preprocess_admissions():
    """
    Preprocesses the ADMISSIONS.csv file and saves it as a Parquet file.
    """
    # Load the ADMISSIONS.csv file
    admissions_df = pd.read_csv("MIMIC_III_Dataset/ADMISSIONS.csv")
    admissions_df = admissions_df.dropna()

    # Convert ADMITTIME and DISCHTIME to datetime
    admissions_df['ADMITTIME'] = pd.to_datetime(admissions_df['ADMITTIME'])
    admissions_df['DISCHTIME'] = pd.to_datetime(admissions_df['DISCHTIME'])

    # Calculate length of stay in days
    admissions_df['LOS'] = (admissions_df['DISCHTIME'] - admissions_df['ADMITTIME']).dt.days

    # Save the preprocessed file as Parquet
    admissions_df.to_parquet("MIMIC_III_Parquet/ADMISSIONS.parquet", index=False)
    admissions_df.to_csv("MIMIC_III_CSV/ADMISSIONS_processed.csv", index=False)
    print("ADMISSIONS.csv preprocessed and saved as csv and Parquet.")

def preprocess_diagnoses_icd():
    """
    Preprocesses the DIAGNOSES_ICD.csv file and saves it as a Parquet file.
    """
    # Load the DIAGNOSES_ICD.csv file
    diagnoses_df = pd.read_csv("MIMIC_III_Dataset/DIAGNOSES_ICD.csv")
    diagnoses_df = diagnoses_df.dropna()

    # Ensure ICD9_CODE is treated as a string
    diagnoses_df['ICD9_CODE'] = diagnoses_df['ICD9_CODE'].astype(str)

    # Save the preprocessed file as Parquet
    diagnoses_df.to_parquet("MIMIC_III_Parquet/DIAGNOSES_ICD.parquet", index=False)
    diagnoses_df.to_csv("MIMIC_III_CSV/DIAGNOSES_ICD_processed.csv", index=False)
    print("D_ICD_DIAGNOSES.csv preprocessed and saved as csv and Parquet.")

def preprocess_icustays():
    """
    Preprocesses the ICUSTAYS.csv file and saves it as a Parquet file.
    """
    # Load the ICUSTAYS.csv file
    icustays_df = pd.read_csv("MIMIC_III_Dataset/ICUSTAYS.csv")
    icustays_df = icustays_df.dropna()

    # Convert INTIME and OUTTIME to datetime
    icustays_df['INTIME'] = pd.to_datetime(icustays_df['INTIME'])
    icustays_df['OUTTIME'] = pd.to_datetime(icustays_df['OUTTIME'])

    # Calculate length of ICU stay in days
    icustays_df['ICU_LOS'] = (icustays_df['OUTTIME'] - icustays_df['INTIME']).dt.days

    # Save the preprocessed file as Parquet
    icustays_df.to_parquet("MIMIC_III_Parquet/ICUSTAYS.parquet", index=False)
    icustays_df.to_csv("MIMIC_III_CSV/ICUSTAYS_processed.csv", index=False)
    print("ICUSTAYS.csv preprocessed and saved as csv and Parquet.")

# ==================================================
# Step 3: Create Hive Tables for Parquet Files
# ==================================================

def create_hive_tables():
    """
    Creates Hive tables for the preprocessed Parquet files.
    """
    # Define Hive commands to create tables
    hive_commands = [
        """
        CREATE EXTERNAL TABLE IF NOT EXISTS patients (
            SUBJECT_ID INT,
            GENDER STRING,
            DOB TIMESTAMP,
            DOD TIMESTAMP,
            AGE INT
        )
        STORED AS PARQUET
        LOCATION '/mimic-iii/PATIENTS.parquet';
        """,
        """
        CREATE EXTERNAL TABLE IF NOT EXISTS admissions (
            HADM_ID INT,
            SUBJECT_ID INT,
            ADMITTIME TIMESTAMP,
            DISCHTIME TIMESTAMP,
            LOS INT
        )
        STORED AS PARQUET
        LOCATION '/mimic-iii/ADMISSIONS.parquet';
        """,
        """
        CREATE EXTERNAL TABLE IF NOT EXISTS diagnoses_icd (
            HADM_ID INT,
            SEQ_NUM INT,
            ICD9_CODE STRING
        )
        STORED AS PARQUET
        LOCATION '/mimic-iii/DIAGNOSES_ICD.parquet';
        """,
        """
        CREATE EXTERNAL TABLE IF NOT EXISTS icustays (
            ICUSTAY_ID INT,
            HADM_ID INT,
            SUBJECT_ID INT,
            INTIME TIMESTAMP,
            OUTTIME TIMESTAMP,
            ICU_LOS DOUBLE
        )
        STORED AS PARQUET
        LOCATION '/mimic-iii/ICUSTAYS.parquet';
        """
    ]

    # Execute Hive commands to create tables
    for command in hive_commands:
        subprocess.run(["hive", "-e", command])

    print("Hive tables created.")

# ==================================================
# Step 4: Run HiveQL Queries for Analysis
# ==================================================

def run_hive_queries():
    """
    Runs HiveQL queries for batch processing.
    """
    # Define HiveQL queries
    hive_queries = [
        """
        -- Average Length of Stay per Diagnosis
        SELECT d.ICD9_CODE, AVG(a.LOS) AS avg_length_of_stay
        FROM admissions a
        JOIN diagnoses_icd d ON a.HADM_ID = d.HADM_ID
        GROUP BY d.ICD9_CODE;
        """,
        """
        -- Distribution of ICU Readmissions
        SELECT COUNT(*) AS readmission_count
        FROM (
            SELECT i.SUBJECT_ID, COUNT(*) AS stay_count
            FROM icustays i
            GROUP BY i.SUBJECT_ID
            HAVING COUNT(*) > 1
        ) AS readmissions;
        """,
        """
        -- Mortality Rates by Demographic Groups
        SELECT p.GENDER, 
               p.AGE,
               COUNT(*) AS total_patients,
               SUM(CASE WHEN a.DISCHTIME IS NOT NULL AND a.DEATHTIME IS NOT NULL THEN 1 ELSE 0 END) AS deceased_patients,
               SUM(CASE WHEN a.DISCHTIME IS NOT NULL AND a.DEATHTIME IS NOT NULL THEN 1 ELSE 0 END) / COUNT(*) AS mortality_rate
        FROM patients p
        JOIN admissions a ON p.SUBJECT_ID = a.SUBJECT_ID
        GROUP BY p.GENDER, p.AGE;
        """
    ]

    # Execute HiveQL queries
    for query in hive_queries:
        subprocess.run(["hive", "-e", query])

    print("HiveQL queries executed.")

# ==================================================
# Main Function to Run the Full Pipeline
# ==================================================

if __name__ == "__main__":
    # Step 1: Preprocess CSV files and save as Parquet
    preprocess_patients()
    preprocess_admissions()
    preprocess_diagnoses_icd()
    preprocess_icustays()
    
    #Step 2: Upload Parquet files to container home
    copy_parquet_files_to_container()
    
    copy_csv_files_to_container()
    
    copy_Map_Red_files_to_container()
    
    # Step 3: Upload Parquet files to HDFS
    upload_to_hdfs_from_container_home()

    # Step 3: Create Hive tables
    create_hive_tables()

    # Step 4: Run HiveQL queries
    run_hive_queries()
    run_hive_queries()

    print("Pipeline execution completed.")