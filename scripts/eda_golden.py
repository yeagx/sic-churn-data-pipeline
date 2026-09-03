"""
scripts/etl_ml_features.py
ML/EDA Extension Branch — builds a WIDE Gold feature table.

Unlike Task 3's official Dim_Customer (which keeps only the brief's
required ~8 columns), this script keeps EVERY customer column, plus
rich per-customer summaries from tickets/offers/usage, for use in
EDA and ML model training.

Reads from:  /user/student/churn/clean/*        (Task 2's output, unchanged)
Writes to:   /user/student/churn_ml/gold/customer_features   (NEW, separate path)
"""

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.window import Window

spark = SparkSession.builder.appName("ETL_ML_Wide_Features").getOrCreate()

CLEAN = "/user/student/churn/clean"
GOLD_ML = "/user/student/churn_ml/gold"

# =====================================================================
# 1. LOAD CLEAN TABLES  (already typed/validated by Task 2 — no rework)
# =====================================================================
customers = spark.read.parquet(f"{CLEAN}/customers")
tickets   = spark.read.parquet(f"{CLEAN}/support_tickets")
offers    = spark.read.parquet(f"{CLEAN}/offers")
usage     = spark.read.parquet(f"{CLEAN}/usage")

# fix: satisfaction_rating arrives as string from the raw source; cast to
# numeric so min()/max()/avg() compare numerically, not lexicographically
tickets = tickets.withColumn(
    "satisfaction_rating", F.col("satisfaction_rating").cast("double"))

print("Loaded:", customers.count(), "customers |",
      tickets.count(), "tickets |", offers.count(), "offers |",
      usage.count(), "usage rows")

# =====================================================================
# 2. TICKETS -> one row per customer  (ALL useful summary stats kept)
# =====================================================================
ticket_agg = tickets.groupBy("customer_id").agg(
    F.count("ticket_id").alias("total_tickets"),
    F.avg("resolution_time_hrs").alias("avg_ticket_res_time_hrs"),
    F.max("resolution_time_hrs").alias("max_ticket_res_time_hrs"),
    F.sum(F.when(F.col("resolution_time_hrs").isNull(), 1).otherwise(0))
        .alias("unresolved_tickets"),
    F.sum(F.when(F.col("severity") == "high", 1).otherwise(0))
        .alias("high_severity_tickets"),
    F.sum(F.when(F.col("severity") == "medium", 1).otherwise(0))
        .alias("medium_severity_tickets"),
    F.sum(F.when(F.col("severity") == "low", 1).otherwise(0))
        .alias("low_severity_tickets"),
    F.countDistinct("issue_type").alias("distinct_issue_types"),
    F.avg("satisfaction_rating").alias("avg_satisfaction_rating"),
    F.min("satisfaction_rating").alias("min_satisfaction_rating"),
    F.max("first_response_ts").alias("last_ticket_date"),
)

# most frequent issue_type per customer (useful categorical feature for EDA)
issue_mode = (tickets.groupBy("customer_id", "issue_type")
    .agg(F.count("*").alias("cnt"))
    .withColumn("rn", F.row_number().over(
        Window.partitionBy("customer_id").orderBy(F.desc("cnt"))))
    .filter(F.col("rn") == 1)
    .select("customer_id", F.col("issue_type").alias("most_common_issue_type")))

ticket_agg = ticket_agg.join(issue_mode, "customer_id", "left")

# =====================================================================
# 3. OFFERS -> one row per customer
# =====================================================================
offer_agg = offers.groupBy("customer_id").agg(
    F.count("offer_id").alias("offers_received"),
    F.sum("accepted").alias("offers_accepted"),
    F.countDistinct("offer_type").alias("distinct_offer_types_received"),
    F.max("date_offered").alias("last_offer_date"),
).withColumn(
    "offer_acceptance_rate",
    F.when(F.col("offers_received") > 0,
           F.round(F.col("offers_accepted") / F.col("offers_received"), 4))
     .otherwise(F.lit(None))
)

# most common offer_type per customer
offer_mode = (offers.groupBy("customer_id", "offer_type")
    .agg(F.count("*").alias("cnt"))
    .withColumn("rn", F.row_number().over(
        Window.partitionBy("customer_id").orderBy(F.desc("cnt"))))
    .filter(F.col("rn") == 1)
    .select("customer_id", F.col("offer_type").alias("most_common_offer_type")))

offer_agg = offer_agg.join(offer_mode, "customer_id", "left")

# =====================================================================
# 4. USAGE -> one row per customer
# =====================================================================
usage_agg = usage.groupBy("customer_id").agg(
    F.count("usage_log_id").alias("total_usage_logs"),
    F.avg("monthly_balance").alias("avg_monthly_balance"),
    F.min("monthly_balance").alias("min_monthly_balance"),
    F.max("monthly_balance").alias("max_monthly_balance"),
    F.stddev("monthly_balance").alias("stddev_monthly_balance"),
    F.avg("num_products").alias("avg_num_products_used"),
    F.max("num_products").alias("max_num_products_used"),
    F.countDistinct("product_type").alias("distinct_products_used"),
    F.min("log_date").alias("first_usage_date"),
    F.max("log_date").alias("last_usage_date"),
)

# recency: months since last activity, relative to a fixed "as-of" date
AS_OF_DATE = "2024-12-31"
usage_agg = usage_agg.withColumn(
    "months_since_last_usage",
    F.round(F.months_between(F.lit(AS_OF_DATE).cast("date"), F.col("last_usage_date")), 1)
)

# =====================================================================
# 5. DERIVE is_churned  (same rule as the official pipeline: no usage
#    in the final 3 months = churned) — kept here too, so the ML table
#    is self-contained and doesn't need the official warehouse table
# =====================================================================
usage_agg = usage_agg.withColumn(
    "is_churned",
    F.when(F.col("months_since_last_usage") >= 3, 1).otherwise(0)
)

# =====================================================================
# 6. JOIN EVERYTHING — customers keeps ALL its original columns
# =====================================================================
wide = (customers
    .join(ticket_agg, "customer_id", "left")
    .join(offer_agg, "customer_id", "left")
    .join(usage_agg, "customer_id", "left")
)

# =====================================================================
# 7. FILL NULLS — only for customers who genuinely had ZERO activity
#    (e.g. 0 tickets ever -> counts should be 0, not null;
#     but rate/average columns stay NULL, since "0 tickets" should not
#     imply "0 average resolution time" — that would be a false signal)
# =====================================================================
count_cols_to_zero = [
    "total_tickets", "unresolved_tickets", "high_severity_tickets",
    "medium_severity_tickets", "low_severity_tickets", "distinct_issue_types",
    "offers_received", "offers_accepted", "distinct_offer_types_received",
    "total_usage_logs", "distinct_products_used",
]
wide = wide.fillna(0, subset=count_cols_to_zero)

# customers with zero usage logs at all (never used the product) -> treat as churned
wide = wide.withColumn(
    "is_churned",
    F.when(F.col("is_churned").isNull(), 1).otherwise(F.col("is_churned"))
)

# =====================================================================
# 8. A FEW EXTRA DERIVED FEATURES useful for EDA/ML (not in the brief,
#    added because you're free to enrich this table however you like)
# =====================================================================
wide = (wide
    .withColumn("clv_ltv",
        F.round(F.col("avg_monthly_balance") * 0.002 * F.col("tenure_months"), 2))
    .withColumn("has_support_history",
        F.when(F.col("total_tickets") > 0, 1).otherwise(0))
    .withColumn("high_value_customer",
        F.when(F.col("balance") > 100000, 1).otherwise(0))
)

# =====================================================================
# 9. FINAL COLUMN CHECK — confirm nothing from `customers` was lost
# =====================================================================
original_customer_cols = set(customers.columns)
final_cols = set(wide.columns)
missing = original_customer_cols - final_cols
print("Customer columns missing from final table (should be EMPTY):", missing)

print("\nFinal schema:")
wide.printSchema()
print("\nFinal row count:", wide.count(),
      "(should equal customers count:", customers.count(), ")")

# =====================================================================
# 10. WRITE — separate path, does not touch the official /warehouse/
# =====================================================================
wide.write.mode("overwrite").parquet(f"{GOLD_ML}/customer_features")
print(f"Wide feature table written to {GOLD_ML}/customer_features")

spark.stop()
