CREATE EXTERNAL TABLE IF NOT EXISTS customer_features_ml (
    customer_id STRING, surname STRING, credit_score INT, geography STRING,
    gender STRING, age INT, tenure_years INT, balance DOUBLE, num_products INT,
    has_cr_card INT, is_active_member INT, estimated_salary DOUBLE,
    exited_source_flag INT, tenure_months INT, date_opened DATE,
    total_tickets BIGINT, avg_ticket_res_time_hrs DOUBLE, max_ticket_res_time_hrs DOUBLE,
    unresolved_tickets BIGINT, high_severity_tickets BIGINT, medium_severity_tickets BIGINT,
    low_severity_tickets BIGINT, distinct_issue_types BIGINT, avg_satisfaction_rating DOUBLE,
    min_satisfaction_rating DOUBLE, last_ticket_date TIMESTAMP, most_common_issue_type STRING,
    offers_received BIGINT, offers_accepted BIGINT, distinct_offer_types_received BIGINT,
    last_offer_date DATE, offer_acceptance_rate DOUBLE, most_common_offer_type STRING,
    total_usage_logs BIGINT, avg_monthly_balance DOUBLE, min_monthly_balance DOUBLE,
    max_monthly_balance DOUBLE, stddev_monthly_balance DOUBLE, avg_num_products_used DOUBLE,
    max_num_products_used INT, distinct_products_used BIGINT, first_usage_date DATE,
    last_usage_date DATE, months_since_last_usage DOUBLE, is_churned INT,
    clv_ltv DOUBLE, has_support_history INT, high_value_customer INT
)
STORED AS PARQUET
LOCATION '/user/student/churn_ml/gold/customer_features';
