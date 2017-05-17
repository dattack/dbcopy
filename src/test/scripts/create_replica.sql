CREATE SCHEMA IF NOT EXISTS hr;

DROP TABLE IF EXISTS hr.emp;

CREATE TABLE IF NOT EXISTS hr.emp (
    ts        TIMESTAMP,
    operation VARCHAR(15),
    mode      INT,
	emp_id    INT,
	emp_name  VARCHAR(50), 
	salary    DOUBLE
);
