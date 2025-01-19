-- Database: admission_db
CREATE DATABASE admission_db;
CREATE TABLE admission_trends (
    time_window TIMESTAMP,
    admission_type VARCHAR(255),
    total_admissions BIGINT
);

-- Database: location_db
CREATE DATABASE location_db;
CREATE TABLE location_trends (
    time_window TIMESTAMP,
    admission_location VARCHAR(255),
    total_admissions BIGINT
);

-- Database: diagnosis_db
CREATE DATABASE diagnosis_db;
CREATE TABLE diagnosis_trends (
    time_window TIMESTAMP,
    diagnosis VARCHAR(255),
    total_admissions BIGINT
);

-- Database: anomalies_db
CREATE DATABASE anomalies_db;
CREATE TABLE readmission_anomalies (
    id BIGINT,
    admission_time TIMESTAMP,
    readmission INT
);
