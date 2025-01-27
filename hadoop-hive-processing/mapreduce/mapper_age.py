# mapper_age.py
import sys

for line in sys.stdin:
    if "AGE" in line:  # Skip AGE in Header Name
        continue
        
    fields = line.strip().split(',')
    age = fields[8]  # Assuming age is in the 9th column
    print(f"age\t{age}")
