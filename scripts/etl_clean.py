"""
scripts/etl_clean.py
Task 2 — Data Cleaning (raw -> clean, Bronze -> Silver)
Reads the 4 raw sources from HDFS, cleans/types/validates each,
writes clean Parquet + rejects orphan rows.
"""

from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql import functions as F

spark = SparkSession.builder.appName("ETL_Clean_Task2").getOrCreate()

RAW = "/user/student/churn/raw"
CLEAN = "/user/student/churn/clean"
REJECT = "/user/student/churn/reject"

# ---------- 1. CUSTOMERS ----------
customers_schema = StructType([
    StructField("row_number_col",     IntegerType()),
    StructField("customer_id",        StringType()),
    StructField("surname",            StringType()),
    StructField("credit_score",       IntegerType()),
    StructField("geography",          StringType()),
    StructField("gender",             StringType()),
    StructField("age",                IntegerType()),
    StructField("tenure_years",       IntegerType()),
    StructField("balance",            DoubleType()),
    StructField("num_products",       IntegerType()),
    StructField("has_cr_card",        IntegerType()),
    StructField("is_active_member",   IntegerType()),
    StructField("estimated_salary",   DoubleType()),
    StructField("exited_source_flag", IntegerType()),
])

customers = spark.read.schema(customers_schema).csv(f"{RAW}/customers")

customers = (customers
    .withColumn("tenure_months", F.col("tenure_years") * 12)
    .withColumn("geography",     F.initcap(F.trim("geography")))
    .withColumn("gender",        F.initcap(F.trim("gender")))
    .withColumn("date_opened",
        F.expr("date_sub(date('2024-12-31'), tenure_years * 365)"))
    .drop("row_number_col"))

customers.write.mode("overwrite").parquet(f"{CLEAN}/customers")
print(f"[customers] cleaned: {customers.count()} rows")

valid_ids = customers.select("customer_id")

# ---------- 2. SUPPORT TICKETS ----------
tickets_raw = spark.read.option("header", True) \
    .option("multiLine", True) \
    .option("quote", '"') \
    .option("escape", '"') \
    .csv(f"{RAW}/support_tickets")

tickets = (tickets_raw
    .withColumnRenamed("Ticket ID", "ticket_id")
    .withColumnRenamed("Customer Satisfaction Rating", "satisfaction_rating")
    .withColumn("issue_type",
        F.lower(F.regexp_replace(F.trim(F.col("Ticket Type")), " ", "_")))
    .withColumn("severity",
        F.lower(F.trim(F.col("Ticket Priority"))))
    .withColumn("ticket_status", F.trim(F.col("Ticket Status")))
    .withColumn("first_response_ts",
        F.to_timestamp(F.col("First Response Time"), "yyyy-MM-dd HH:mm:ss"))
    .withColumn("resolution_ts",
        F.to_timestamp(F.col("Time to Resolution"), "yyyy-MM-dd HH:mm:ss"))
    .withColumn("resolution_time_hrs",
        F.round((F.unix_timestamp("resolution_ts") - F.unix_timestamp("first_response_ts")) / 3600.0, 2))
    .select("ticket_id", "customer_id", "issue_type", "severity", "ticket_status",
            "first_response_ts", "resolution_ts", "resolution_time_hrs", "satisfaction_rating"))

tickets_valid = tickets.join(valid_ids, "customer_id", "inner")
tickets_invalid = tickets.join(valid_ids, "customer_id", "left_anti")

tickets_invalid.write.mode("overwrite").parquet(f"{REJECT}/support_tickets")
tickets_valid.write.mode("overwrite").parquet(f"{CLEAN}/support_tickets")
print(f"[support_tickets] valid: {tickets_valid.count()}, rejected: {tickets_invalid.count()}")

# ---------- 3. OFFERS ----------
offers_raw = spark.read.option("header", True).csv(f"{RAW}/offers")

offers = (offers_raw
    .withColumn("offer_id", F.trim(F.col("offer_id")))
    .withColumn("customer_id", F.trim(F.col("customer_id")))
    .withColumn("offer_type", F.lower(F.trim(F.col("offer_type"))))
    .withColumn("accepted", F.col("accepted").cast("int"))
    .withColumn("date_offered", F.to_date(F.col("date_offered"), "yyyy-MM-dd")))

offers_valid = offers.join(valid_ids, "customer_id", "inner")
offers_invalid = offers.join(valid_ids, "customer_id", "left_anti")

offers_invalid.write.mode("overwrite").parquet(f"{REJECT}/offers")
offers_valid.write.mode("overwrite").parquet(f"{CLEAN}/offers")
print(f"[offers] valid: {offers_valid.count()}, rejected: {offers_invalid.count()}")

# ---------- 4. USAGE ----------
usage_raw = spark.read.json(f"{RAW}/usage_json")

usage = (usage_raw
    .withColumn("log_date", F.to_date(F.col("log_date"), "yyyy-MM-dd"))
    .withColumn("log_month", F.date_format(F.col("log_date"), "yyyy-MM"))
    .withColumn("num_products", F.col("num_products").cast("int")))

usage_valid = usage.join(valid_ids, "customer_id", "inner")
usage_invalid = usage.join(valid_ids, "customer_id", "left_anti")

usage_invalid.write.mode("overwrite").parquet(f"{REJECT}/usage")
usage_valid.write.mode("overwrite").parquet(f"{CLEAN}/usage")
print(f"[usage] valid: {usage_valid.count()}, rejected: {usage_invalid.count()}")

print("=== Task 2 cleaning complete ===")
spark.stop()
