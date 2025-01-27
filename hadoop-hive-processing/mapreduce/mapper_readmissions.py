# mapper_readmissions.py
import sys

from collections import defaultdict

patient_admissions = defaultdict(int)

for line in sys.stdin:
    fields = line.strip().split(',')
    subject_id = fields[1]  # Assuming subject_id is in the 1st column
    patient_admissions[subject_id] += 1

for subject_id, count in patient_admissions.items():
    print(f"{count}\t1")