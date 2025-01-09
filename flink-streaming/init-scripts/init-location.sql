\c location_db
CREATE TABLE location_trends (
    time_window TIMESTAMP(3),
    admission_location VARCHAR(255),
    total_admissions BIGINT
);
