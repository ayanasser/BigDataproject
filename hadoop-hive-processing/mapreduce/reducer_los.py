# reducer_los.py
import sys

current_diagnosis = None
total_los = 0
count = 0

for line in sys.stdin:
    diagnosis, los = line.strip().split('\t')
    los = int(los)

    if diagnosis == current_diagnosis:
        total_los += los
        count += 1
    else:
        if current_diagnosis:
            print(f"{current_diagnosis}\t{total_los / count}")
        current_diagnosis = diagnosis
        total_los = los
        count = 1

if current_diagnosis:
    print(f"{current_diagnosis}\t{total_los / count}")