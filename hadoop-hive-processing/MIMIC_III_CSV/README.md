# Preprocessed CSV Data Files

## Overview

The preprocessed CSV data files are available in this directory. These files have been generated after running the data processing pipeline and include clean, structured data ready for further analysis.

## How to Extract the CSV Files

To generate the preprocessed CSV files, execute the **Full_Pipeline_Data_Processing** script, which is located in the `Hadoop_for_Distributed_Storage` directory. 
This script utilizes Hadoop's distributed storage capabilities to process large datasets efficiently and outputs clean CSV files.

## Output Details

The output CSV files include the following:

1. **ADMISSIONS_preprocessed.csv**  
   - Contains cleaned and structured data related to hospital admissions, including admission times, discharge times, and admission types.

2. **DIAGNOSES_ICD_preprocessed.csv**  
   - Includes processed diagnosis codes and descriptions based on the International Classification of Diseases (ICD).

3. **ICUSTAYS_preprocessed.csv**  
   - Provides preprocessed ICU stay records, including stay durations, start and end times, and patient IDs.

4. **PATIENTS_preprocessed.csv**  
   - Features demographic and identification details of patients, cleaned and organized for analysis.

5. **CHARTEVENTS_preprocessed.csv**  
   - Contains time-stamped clinical data recorded during ICU stays, such as vital signs and lab results, cleaned and optimized for usability.

## Notes

- The preprocessing pipeline ensures that the data is free from duplicates, inconsistencies, and missing values to the extent possible.
- These CSV files are ready for analytical workflows, including statistical analysis, machine learning, or visualization tasks.
- For large-scale processing, the pipeline leverages Hadoop's distributed computing capabilities, ensuring efficiency and scalability for big data tasks.
