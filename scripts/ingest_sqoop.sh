#!/bin/bash
# scripts/ingest_sqoop.sh
# Ingests MySQL customers table into HDFS /user/student/churn/raw/customers via Apache Sqoop

set -e

MYSQL_USER="${MYSQL_USER:-root}"
MYSQL_HOST="${MYSQL_HOST:-localhost}"
MYSQL_DB="churn_db"
TARGET_HDFS_DIR="/user/student/churn/raw/customers"

echo "=== Sqoop Ingestion: MySQL -> HDFS ==="
echo "Source: mysql://${MYSQL_HOST}/${MYSQL_DB}/customers"
echo "Destination: ${TARGET_HDFS_DIR}"
echo "Mappers: 4 (split by customer_id)"

if [ -n "$MYSQL_PASSWORD" ]; then
    sqoop import \
      -Dorg.apache.sqoop.splitter.allow_text_splitter=true \
      --connect "jdbc:mysql://${MYSQL_HOST}/${MYSQL_DB}" \
      --username "${MYSQL_USER}" \
      --password "${MYSQL_PASSWORD}" \
      --table customers \
      --target-dir "${TARGET_HDFS_DIR}" \
      --delete-target-dir \
      --num-mappers 4 \
      --split-by customer_id \
      --as-textfile
else
    echo "Prompting for MySQL password:"
    sqoop import \
      -Dorg.apache.sqoop.splitter.allow_text_splitter=true \
      --connect "jdbc:mysql://${MYSQL_HOST}/${MYSQL_DB}" \
      --username "${MYSQL_USER}" \
      -P \
      --table customers \
      --target-dir "${TARGET_HDFS_DIR}" \
      --delete-target-dir \
      --num-mappers 4 \
      --split-by customer_id \
      --as-textfile
fi

echo "=== Sqoop Ingestion Complete ==="
echo "Verifying HDFS output:"
hdfs dfs -ls "${TARGET_HDFS_DIR}"
hdfs dfs -cat "${TARGET_HDFS_DIR}/part-m-00000" | head -n 5
