# reducer_age.py
import sys

total_age = 0
count = 0

for line in sys.stdin:
    key, value = line.strip().split('\t')
    total_age += int(value)
    count += 1

if count > 0:
    print(f"Average Age\t{total_age / count}")
