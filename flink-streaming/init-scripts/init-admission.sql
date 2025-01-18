\c admission_db
CREATE TABLE admission_trends (
    time_window TIMESTAMP(3),
    admission_type VARCHAR(255),
    total_admissions BIGINT
);
