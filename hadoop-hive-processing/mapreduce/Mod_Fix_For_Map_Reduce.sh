# First, verify you're in the correct directory
pwd

# Create the mapper file
cat > mapper_age.py << 'EOL'
# mapper_age.py
import sys

for line in sys.stdin:
    if "AGE" in line:  # Skip AGE in Header Name
        continue
        
    fields = line.strip().split(',')
    age = fields[8]  # Assuming age is in the 9th column
    print(f"age\t{age}")
EOL

cat > mapper_los.py << 'EOL'
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
EOL

cat > mapper_readmissions.py<< 'EOL'
import sys

from collections import defaultdict

patient_admissions = defaultdict(int)

for line in sys.stdin:
    fields = line.strip().split(',')
    subject_id = fields[1]  # Assuming subject_id is in the 1st column
    patient_admissions[subject_id] += 1

for subject_id, count in patient_admissions.items():
    print(f"{count}\t1").py 
EOL

cat > reducer_age.py << 'EOL'
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
EOL

cat > reducer_los.py << 'EOL'
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
EOL

cat > reducer_readmissions.py << 'EOL'
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
EOL

# Make the files executable
chmod +x mapper_age.py mapper_los.py mapper_readmissions.py reducer_age.py reducer_los.py reducer_readmissions.py
