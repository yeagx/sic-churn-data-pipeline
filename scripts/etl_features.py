"""
scripts/etl_features.py
Feature Engineering (Silver -> Gold)

Reads the 4 CLEAN tables from HDFS, aggregates them to one row per
customer, derives the churn features, and writes the final
Dim_Customer table (partitioned Parquet) to the warehouse zone.
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

spark = SparkSession.builder.appName("ETL_Features").getOrCreate()


CLEAN = "/user/student/churn/clean"
WAREHOUSE = "/user/student/churn/warehouse"

REFERENCE_DATE = "2024-12-31"      # the notional "today" of the project
MARGIN_RATE = 0.002                # assumed 0.2% monthly margin, for clv_ltv
LAST_3_MONTHS = ["2024-10", "2024-11", "2024-12"]  # last 3 months of the 24-month usage window

# Read the clean tables
customers = spark.read.parquet(f"{CLEAN}/customers")
tickets = spark.read.parquet(f"{CLEAN}/support_tickets")
offers = spark.read.parquet(f"{CLEAN}/offers")
usage = spark.read.parquet(f"{CLEAN}/usage")

customers.cache()
usage.cache()

# Aggregate support tickets to one row per customer
ticket_agg = tickets.groupBy("customer_id").agg(
    F.avg("resolution_time_hrs").alias("avg_ticket_res_time_hrs"),
    F.count("ticket_id").alias("total_tickets"),
    F.sum(
        F.when(F.col("severity").isin("high", "critical"), 1).otherwise(0)
    ).alias("high_severity_tickets"),
)


# Aggregate offers to one row per customer
offer_agg = (
    offers.groupBy("customer_id")
    .agg(
        F.count("offer_id").alias("offers_received"),
        F.sum(F.when(F.col("accepted") == 1, 1).otherwise(0)).alias("offers_accepted"),
    )
    .withColumn(
        "offer_acceptance_rate",
        F.when(
            F.col("offers_received") > 0,
            F.col("offers_accepted") / F.col("offers_received"),
        ).otherwise(F.lit(None).cast("double")),
    )
)

# Aggregate usage to one row per customer
usage_summary = usage.groupBy("customer_id").agg(
    F.avg("monthly_balance").alias("avg_monthly_balance"),
    F.max("log_month").alias("last_active_month"),
)

# rank each customer's records newest-first and keep rank 1
recency_window = Window.partitionBy("customer_id").orderBy(F.col("log_date").desc())
usage_latest = (
    usage.withColumn("rn", F.row_number().over(recency_window))
    .filter(F.col("rn") == 1)
    .select("customer_id", "num_products")
)

# How many usage records did this customer have in the LAST 3 MONTHS
# of the window? This is what tells us if they churned.
recent_activity = (
    usage.filter(F.col("log_month").isin(LAST_3_MONTHS))
    .groupBy("customer_id")
    .agg(F.count("usage_log_id").alias("records_last_3_months"))
)

usage_agg = (
    usage_summary.join(usage_latest, "customer_id", "left")
    .join(recent_activity, "customer_id", "left")
    .fillna({"records_last_3_months": 0})
)

# Derive is_churned and churn_month (Section 4.4 rule)

usage_agg = usage_agg.withColumn(
    "is_churned",
    F.when(F.col("records_last_3_months") == 0, 1).otherwise(0),
)

usage_agg = usage_agg.withColumn(
    "churn_month",
    F.when(F.col("is_churned") == 1, F.col("last_active_month")).otherwise(
        F.lit(None).cast("string")
    ),
)

usage_agg = usage_agg.drop("records_last_3_months", "last_active_month")


# Join everything onto the customer profile
# Only keep the customer columns we actually need in the final table.

customers_base = customers.select(
    "customer_id", "age", "gender", "geography", "tenure_months", "credit_score"
)


dim = (
    F.broadcast(customers_base)
    .join(ticket_agg, "customer_id", "left")
    .join(offer_agg, "customer_id", "left")
    .join(usage_agg, "customer_id", "left")
)


dim = dim.fillna(
    {
        "total_tickets": 0,
        "high_severity_tickets": 0,
        "offers_received": 0,
        "offers_accepted": 0,
    }
)


# Derive clv_ltv 
dim = dim.withColumn(
    "clv_ltv",
    F.round(F.col("avg_monthly_balance") * F.lit(MARGIN_RATE) * F.col("tenure_months"), 2),
)

# Surrogate key
key_window = Window.orderBy("customer_id")
dim = dim.withColumn("cust_key", F.row_number().over(key_window))

dim = (
    dim.withColumn("dw_start_date", F.to_date(F.lit(REFERENCE_DATE)))
    .withColumn("dw_end_date", F.to_date(F.lit("9999-12-31")))
    .withColumn("is_current", F.lit(1))
)

# Final column order AND types
dim_final = dim.select(
    F.col("cust_key").cast("bigint"),
    F.col("customer_id").cast("string"),
    F.col("age").cast("int"),
    F.col("gender").cast("string"),
    F.col("geography").cast("string"),
    F.col("tenure_months").cast("int"),
    F.col("credit_score").cast("int"),
    F.col("num_products").cast("int"),
    F.col("is_churned").cast("int"),
    F.col("churn_month").cast("string"),
    F.col("clv_ltv").cast("double"),
    F.col("avg_monthly_balance").cast("double"),
    F.col("avg_ticket_res_time_hrs").cast("double"),
    F.col("total_tickets").cast("int"),
    F.col("high_severity_tickets").cast("int"),
    F.col("offers_received").cast("int"),
    F.col("offers_accepted").cast("int"),
    F.col("offer_acceptance_rate").cast("double"),
    F.col("dw_start_date").cast("date"),
    F.col("dw_end_date").cast("date"),
    F.col("is_current").cast("int"),
)

# Write partitioned Parquet to the warehouse zone
dim_final.write.mode("overwrite").partitionBy("geography").parquet(
    f"{WAREHOUSE}/dim_customer"
)

total_rows = dim_final.count()
distinct_customers = dim_final.select("customer_id").distinct().count()

duplicate_keys = dim_final.groupBy("cust_key").count().filter("count > 1").count()

print(f"[dim_customer] total rows: {total_rows}")
print(f"[dim_customer] distinct customer_id: {distinct_customers}")
print(f"[dim_customer] duplicate cust_key groups: {duplicate_keys}")
dim_final.groupBy("geography").count().show()

agreement = (
    customers.join(dim_final, "customer_id")
    .filter(F.col("exited_source_flag") == F.col("is_churned"))
    .count()
)
print(
    f"is_churned vs exited_source_flag agreement: "
    f"{agreement}/{total_rows} "
    f"({round(100 * agreement / total_rows, 2)}%)"
)

print("=== Task 3 feature engineering complete ===")
spark.stop()