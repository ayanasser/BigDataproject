# mapper_los.py
import sys
from datetime import datetime

for line in sys.stdin:
    if "ADMITTIME" in line:  # Skip Header
        continue
        
    fields = line.strip().split(',')
    admittime = datetime.strptime(fields[3], '%Y-%m-%d %H:%M:%S')  # Assuming admittime is in the 3rd column
    dischtime = datetime.strptime(fields[4], '%Y-%m-%d %H:%M:%S')  # Assuming dischtime is in the 4th column
    diagnosis = fields[5]  # Assuming diagnosis is in the 5th column
    length_of_stay = (dischtime - admittime).days
    print(f"{diagnosis}\t{length_of_stay}")