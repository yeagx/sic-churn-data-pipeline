# sic-churn-data-pipeline
End-to-end big data pipeline for bank customer churn analysis. Ingests four sources via NiFi and Sqoop into HDFS, transforms with PySpark, and serves a star-schema dimension table through Hive.
