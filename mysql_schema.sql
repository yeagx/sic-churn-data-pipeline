-- sql/mysql_schema.sql
-- Customer Churn Data Pipeline — Task 1: MySQL Schema & Customer Ingestion

CREATE DATABASE IF NOT EXISTS churn_db;
USE churn_db;

DROP TABLE IF EXISTS customers;

CREATE TABLE customers (
  row_number_col   INT,
  customer_id      VARCHAR(20) PRIMARY KEY,
  surname          VARCHAR(100),
  credit_score     INT,
  geography        VARCHAR(50),
  gender           VARCHAR(10),
  age              INT,
  tenure           INT,
  balance          DECIMAL(15,2),
  num_products     INT,
  has_cr_card      TINYINT,
  is_active_member TINYINT,
  estimated_salary DECIMAL(15,2),
  exited           TINYINT
);

-- Note: In MySQL, ensure local-infile is enabled:
-- SET GLOBAL local_infile = 1;
-- Run with: mysql --local-infile=1 -u root -p < sql/mysql_schema.sql

LOAD DATA LOCAL INFILE 'data/Churn_Modelling.csv'
INTO TABLE customers
FIELDS TERMINATED BY ',' 
OPTIONALLY ENCLOSED BY '"'
LINES TERMINATED BY '\n' 
IGNORE 1 ROWS
(row_number_col, customer_id, surname, credit_score, geography, gender, age, tenure, balance, num_products, has_cr_card, is_active_member, estimated_salary, exited);

-- Verification:
SELECT COUNT(*) AS total_customers FROM customers;
SELECT exited, COUNT(*) AS customer_count FROM customers GROUP BY exited;
