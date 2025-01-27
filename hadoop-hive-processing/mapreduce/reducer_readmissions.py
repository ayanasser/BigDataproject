# reducer_readmissions.py
import sys

current_count = None
total_patients = 0

for line in sys.stdin:
    count, patients = line.strip().split('\t')
    patients = int(patients)

    if count == current_count:
        total_patients += patients
    else:
        if current_count:
            print(f"{current_count}\t{total_patients}")
        current_count = count
        total_patients = patients

if current_count:
    print(f"{current_count}\t{total_patients}")