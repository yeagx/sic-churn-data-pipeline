#!/bin/bash
###############################################################################
# Customer Churn Data Pipeline — full end-to-end run
# SIC Project 1, section 1.1
#
# Runs every stage: source data -> MySQL -> Sqoop -> HDFS -> PySpark clean ->
# PySpark features -> Hive warehouse -> Monthly Churn Rate KPI.
#
# Usage:  bash run_pipeline.sh
#
# Prerequisites (not done by this script):
#   - Hadoop running:  su - hadoop && start-dfs.sh && start-yarn.sh && exit
#   - MariaDB running: sudo systemctl start mariadb
###############################################################################

set -e

# ---------------------------------------------------------------- config ----
REPO="$HOME/sic-churn-data-pipeline"
DATA="$HOME/data"
INBOX="$HOME/nifi_inbox"
HDFS_BASE="/user/student/churn"

export MYSQL_USER="${MYSQL_USER:-root}"
export MYSQL_PASSWORD="${MYSQL_PASSWORD:-student}"
export MYSQL_HOST="${MYSQL_HOST:-localhost}"

# The SIC VM sets PYSPARK_DRIVER_PYTHON=jupyter, which makes spark-submit
# hand the script to Jupyter instead of running it. Override for this session.
export PYSPARK_DRIVER_PYTHON=python3
unset  PYSPARK_DRIVER_PYTHON_OPTS

banner() { echo; echo "=============================================================="; echo "  $1"; echo "=============================================================="; }

cd "$REPO"
START=$(date +%s)

# ------------------------------------------------------- 1. source data ----
banner "STAGE 1/7  Generating source data (seed 42, reproducible)"

mkdir -p "$DATA" "$INBOX"
cp -f data_sources/Churn_Modelling.csv "$HOME/Churn_Modelling.csv"

# generate_data.py creates offers.csv and usage.jsonl, then attempts to re-key
# the support tickets from the raw Kaggle download. That raw file is not in the
# repo (only the finished re-keyed output is), so step 4 exits non-zero. The
# output it would produce is already committed, so this is expected.
python3 scripts/generate_data.py || echo "  [note] ticket re-key step skipped - using committed support_tickets_rekeyed.csv"
cp -f data_sources/support_tickets_rekeyed.csv "$DATA/"

echo "  offers.csv  : $(wc -l < "$DATA/offers.csv") lines"
echo "  usage.jsonl : $(wc -l < "$DATA/usage.jsonl") lines"

# ------------------------------------------------------- 2. hdfs layout ----
banner "STAGE 2/7  Preparing HDFS zones"

hdfs dfs -mkdir -p "$HDFS_BASE"/raw/{customers,support_tickets,offers,usage_json}
hdfs dfs -mkdir -p "$HDFS_BASE"/clean/{customers,support_tickets,offers,usage}
hdfs dfs -mkdir -p "$HDFS_BASE"/reject/{support_tickets,offers,usage}
hdfs dfs -mkdir -p "$HDFS_BASE"/warehouse/dim_customer
hdfs dfs -ls "$HDFS_BASE"

# ------------------------------------------- 3. ingestion: mysql + sqoop ----
banner "STAGE 3/7  Ingestion path A - MySQL to HDFS via Apache Sqoop"

mysql --local-infile=1 -u "$MYSQL_USER" -p"$MYSQL_PASSWORD" < sql/mysql_schema.sql
bash scripts/ingest_sqoop.sh

echo "  rows landed by sqoop: $(hdfs dfs -cat "$HDFS_BASE"/raw/customers/part-m-* | wc -l)"

# ------------------------------------------------ 4. ingestion: file feed ----
banner "STAGE 4/7  Ingestion path B - files to HDFS"

# The usage log is split into ~113 chunks so the feed behaves like a stream
# rather than one bulk copy.
rm -f "$INBOX"/usage_*.jsonl
split -l 2000 "$DATA/usage.jsonl" "$INBOX/usage_" --additional-suffix=.jsonl
cp -f "$DATA/offers.csv" "$DATA/support_tickets_rekeyed.csv" "$INBOX/"
echo "  staged $(ls "$INBOX" | wc -l) files in $INBOX"

# NiFi (GetFile -> PutHDFS) consumes this folder when running. Documented
# fallback below performs the identical byte-for-byte delivery.
hdfs dfs -put -f "$INBOX"/usage_*.jsonl          "$HDFS_BASE/raw/usage_json/"
hdfs dfs -put -f "$DATA/offers.csv"              "$HDFS_BASE/raw/offers/"
hdfs dfs -put -f "$DATA/support_tickets_rekeyed.csv" "$HDFS_BASE/raw/support_tickets/"

# ---------------------------------------------------------- 5. cleaning ----
banner "STAGE 5/7  PySpark cleaning  (raw -> clean, Parquet)"

spark-submit scripts/etl_clean.py 2>/dev/null | grep -E "^\[|Task 2"

# ------------------------------------------------- 6. feature engineering ----
banner "STAGE 6/7  PySpark feature engineering  (clean -> warehouse)"

spark-submit scripts/etl_features.py 2>/dev/null \
  | grep -E "^\[|===|Germany|France|Spain|geography|agreement"

# ------------------------------------------------- 7. warehouse and KPI ----
banner "STAGE 7/7  Hive warehouse and Monthly Churn Rate KPI"

hive -S -f sql/hive_ddl.sql 2>/dev/null

echo
echo "--- warehouse verification ---"
hive -S -e "
SHOW PARTITIONS churn_dw.dim_customer;
SELECT COUNT(*) AS total_rows FROM churn_dw.dim_customer;
SELECT geography, COUNT(*) FROM churn_dw.dim_customer GROUP BY geography;
SELECT SUM(is_churned) AS churned_customers FROM churn_dw.dim_customer;" 2>/dev/null

echo
echo "--- KPI: Monthly Churn Rate ---"
echo "month     churned  active_at_start  rate_pct"
hive -S -f sql/kpi_monthly_churn.sql 2>/dev/null

# ------------------------------------------------------------------ done ----
END=$(date +%s)
banner "PIPELINE COMPLETE in $(( (END-START)/60 ))m $(( (END-START)%60 ))s"
