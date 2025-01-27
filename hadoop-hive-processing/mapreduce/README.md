# Hadoop Data Processing Framework

This repository contains a comprehensive set of mapper and reducer scripts designed for processing and analyzing healthcare data in a Hadoop distributed computing environment. The framework enables efficient processing of patient data, including age demographics, length of stay analysis, and readmission patterns.

## Architecture Overview

The system consists of paired mapper and reducer scripts that work in conjunction to process and aggregate data. Each mapper processes specific data fields from the input dataset, while its corresponding reducer aggregates and summarizes the results.

## Project Structure

```
.
├── mapper_age.py
├── mapper_los.py
├── mapper_readmissions.py
├── reducer_age.py
├── reducer_los.py
├── reducer_readmissions.py
└── README.md
```

## Prerequisites

The project uses Docker to provide a pre-configured Hadoop environment in standalone mode, simplifying setup and ensuring consistency across different systems.

### Docker Setup

1. Install Docker following the [official Docker installation guide](https://docs.docker.com/get-docker/)
2. Verify installation:
   ```bash
   docker --version
   ```

### Hadoop Environment Setup

1. Pull the Hadoop Docker image:
   ```bash
   docker image pull twkdocker/nuf23461-hadoop:latest
   ```
2. Create and access container:
   ```bash
   docker container run -it --name <container_name> twkdocker/nuf23461-hadoop:latest
   docker exec -it <container_name> bash
   ```

## Processing Components

### Age Analysis (mapper_age.py & reducer_age.py)

**Mapper Purpose:**
- Extracts age information from the dataset
- Groups age data for aggregation and analysis

**Mapper Implementation:**
- Skips header rows containing "AGE"
- Processes CSV input, extracting age from column 9
- Outputs age values with tab delimiters

**Reducer Purpose:**
- Aggregates age data from mapper output
- Calculates overall average age
- Provides a single summary statistic

**Reducer Implementation:**
```python
import sys
total_age = 0
count = 0
for line in sys.stdin:
    key, value = line.strip().split('\t')
    total_age += int(value)
    count += 1
if count > 0:
    print(f"Average Age\t{total_age / count}")
```

### Length of Stay Analysis (mapper_los.py & reducer_los.py)

**Mapper Purpose:**
- Computes patient length of stay
- Associates LOS with diagnostic information
- Enables analysis of stay duration by diagnosis

**Mapper Implementation:**
- Processes admission/discharge times (columns 4 and 5)
- Extracts diagnostic information (column 6)
- Calculates LOS in days

**Reducer Purpose:**
- Aggregates length of stay by diagnosis
- Calculates average LOS for each diagnostic category
- Enables analysis of stay duration patterns

**Reducer Implementation:**
```python
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
```

### Readmission Analysis (mapper_readmissions.py & reducer_readmissions.py)

**Mapper Purpose:**
- Identifies repeat hospital visits
- Tracks admission frequency by patient
- Enables readmission pattern analysis

**Mapper Implementation:**
- Processes patient identifiers (column 2)
- Maintains admission counts per patient
- Uses defaultdict for efficient tracking

**Reducer Purpose:**
- Aggregates readmission counts
- Provides distribution of admission frequencies
- Enables identification of high-frequency admissions

**Reducer Implementation:**
```python
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
```

## Usage

### Testing Individual Components

Test mappers and reducers standalone:
```bash
# Test mapper
cat input_file.csv | python mapper.py

# Test complete pipeline
cat input_file.csv | python mapper.py | sort | python reducer.py
```

### Running in Hadoop

Execute the complete pipeline using Hadoop Streaming:
```bash
hadoop jar $HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-*.jar \
    -input /path/to/input \
    -output /path/to/output \
    -mapper /path/to/mapper.py \
    -reducer /path/to/reducer.py \
    -file /path/to/mapper.py \
    -file /path/to/reducer.py
```

## Installation

1. Copy all scripts to your Hadoop environment:
```bash
# First, verify directory
pwd

# Create mapper files
cat > mapper_age.py << 'EOL'
# [age mapper code]
EOL

cat > mapper_los.py << 'EOL'
# [los mapper code]
EOL

cat > mapper_readmissions.py << 'EOL'
# [readmissions mapper code]
EOL

# Create reducer files
cat > reducer_age.py << 'EOL'
# [age reducer code]
EOL

cat > reducer_los.py << 'EOL'
# [los reducer code]
EOL

cat > reducer_readmissions.py << 'EOL'
# [readmissions reducer code]
EOL

# Make all files executable
chmod +x mapper_*.py reducer_*.py
```

## Contributing

When contributing new components or modifications:

1. Follow existing naming conventions
2. Document input assumptions and output formats
3. Include error handling for malformed input
4. Test with corresponding mapper/reducer pairs
5. Verify output format consistency
6. Add appropriate comments and documentation

## Technical Notes

- Mappers expect CSV-formatted input data
- Reducers expect tab-delimited input
- Input must be sorted by key for reducers (Hadoop handles this automatically)
- Output is written to HDFS in tab-delimited format
- Error handling assumes valid numeric input
- Monitor memory usage when processing large datasets
- Each mapper corresponds to a specific reducer
