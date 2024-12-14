import pandas as pd
import os

# Define input and output directories
data_dir = "path/to/mimic-iii-dataset"
output_dir = "path/to/output-directory"

# Function to load CSV files
def load_csv(filename):
    """
    Load a CSV file into a Pandas DataFrame.
    Args:
        filename (str): Name of the CSV file.
    Returns:
        pd.DataFrame: Loaded data.
    """
    filepath = os.path.join(data_dir, filename)
    return pd.read_csv(filepath)

# Step 1: Load and Clean Patient Data
def process_patients():
    """
    Load, clean, and save patient data in a big data-friendly format.
    """
    print("Processing Patient Data...")
    patients = load_csv("PATIENTS.csv")

    # Clean and Normalize Data
    patients["DOB"] = pd.to_datetime(patients["DOB"], errors="coerce")
    patients["DOD"] = pd.to_datetime(patients["DOD"], errors="coerce")
    patients.dropna(subset=["SUBJECT_ID"], inplace=True)

    # Save Cleaned Data to Parquet
    output_path = os.path.join(output_dir, "patients.parquet")
    patients.to_parquet(output_path, index=False)
    print(f"Patients data cleaned and saved to {output_path}.")

# Step 2: Load and Clean Admissions Data
def process_admissions():
    """
    Load, clean, and save admissions data in a big data-friendly format.
    """
    print("Processing Admissions Data...")
    admissions = load_csv("ADMISSIONS.csv")

    # Clean Data
    admissions["ADMITTIME"] = pd.to_datetime(admissions["ADMITTIME"], errors="coerce")
    admissions["DISCHTIME"] = pd.to_datetime(admissions["DISCHTIME"], errors="coerce")
    admissions["DEATHTIME"] = pd.to_datetime(admissions["DEATHTIME"], errors="coerce")

    # Save Cleaned Data to Parquet
    output_path = os.path.join(output_dir, "admissions.parquet")
    admissions.to_parquet(output_path, index=False)
    print(f"Admissions data cleaned and saved to {output_path}.")

# Step 3: Load and Clean Lab Events Data
def process_lab_events():
    """
    Load, clean, and save lab events data in a big data-friendly format.
    """
    print("Processing Lab Events Data...")
    lab_events = load_csv("LABEVENTS.csv")

    # Filter relevant columns
    lab_events = lab_events[["SUBJECT_ID", "ITEMID", "CHARTTIME", "VALUE", "VALUENUM"]]
    lab_events["CHARTTIME"] = pd.to_datetime(lab_events["CHARTTIME"], errors="coerce")

    # Save Cleaned Data to Parquet
    output_path = os.path.join(output_dir, "lab_events.parquet")
    lab_events.to_parquet(output_path, index=False)
    print(f"Lab Events data cleaned and saved to {output_path}.")

# Step 4: Run All Processing Functions
def main():
    """
    Main function to process all datasets.
    """
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    process_patients()
    process_admissions()
    process_lab_events()
    print("All datasets cleaned and saved in Parquet format.")

if __name__ == "__main__":
    main()
