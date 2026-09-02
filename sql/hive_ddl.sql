-- Deliverable 3: Hive DDL for the Dim_Customer warehouse table
-- External table over the Parquet output of scripts/etl_features.py
-- Partitioned by geography (3 balanced values: France, Germany, Spain)

CREATE DATABASE IF NOT EXISTS churn_dw;

DROP TABLE IF EXISTS churn_dw.dim_customer;

CREATE EXTERNAL TABLE churn_dw.dim_customer (
  cust_key                BIGINT  COMMENT 'Surrogate key, row_number over customer_id',
  customer_id             STRING  COMMENT 'Business key from source system',
  age                     INT,
  gender                  STRING,
  tenure_months           INT,
  credit_score            INT,
  num_products            INT     COMMENT 'Latest value from usage log',
  is_churned              INT     COMMENT 'Target variable: no usage in final 3 months',
  churn_month             STRING  COMMENT 'Month of last usage record, NULL if active',
  clv_ltv                 DOUBLE  COMMENT 'avg_monthly_balance * 0.002 * tenure_months',
  avg_monthly_balance     DOUBLE,
  avg_ticket_res_time_hrs DOUBLE  COMMENT 'NULL where customer has no tickets',
  total_tickets           INT,
  high_severity_tickets   INT,
  offers_received         INT,
  offers_accepted         INT,
  offer_acceptance_rate   DOUBLE,
  dw_start_date           DATE    COMMENT 'SCD-2 validity start',
  dw_end_date             DATE    COMMENT 'SCD-2 validity end, 9999-12-31 while current',
  is_current              INT     COMMENT 'SCD-2 current-row flag'
)
PARTITIONED BY (geography STRING)
STORED AS PARQUET
LOCATION '/user/student/churn/warehouse/dim_customer';

MSCK REPAIR TABLE churn_dw.dim_customer;
