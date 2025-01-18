\c diagnosis_db
CREATE TABLE diagnosis_trends (
    time_window TIMESTAMP(3),
    diagnosis VARCHAR(255),
    total_admissions BIGINT
);
