# Apache NiFi Ingestion Guide (Task 1)

This guide explains how to import, configure, run, and export the NiFi flow to land support tickets, offers, and usage logs into HDFS raw zone.

---

## 1. Staging Files into NiFi Inbox

On the Linux VM (as user `student`):

```bash
# 1. Create the NiFi inbox folder
mkdir -p ~/nifi_inbox

# 2. Chunk the large usage.jsonl file so NiFi streams multiple chunks
split -l 2000 data/usage.jsonl ~/nifi_inbox/usage_ --additional-suffix=.jsonl

# 3. Copy offers and re-keyed tickets to the inbox
cp data/offers.csv ~/nifi_inbox/
cp data/support_tickets_rekeyed.csv ~/nifi_inbox/
```

---

## 2. Setting Up NiFi Canvas

1. Open Apache NiFi Web UI in your browser (typically `http://localhost:8080/nifi` or `http://<VM-IP>:8080/nifi`).
2. **Option A (Import Template)**:
   - Click the **Upload Template** icon in the Operate Palette on the left.
   - Select `nifi/churn_ingestion_flow.xml` from this repository.
   - Drag the **Template** icon from the top toolbar onto the canvas and choose `churn_ingestion_flow`.
3. **Option B (Build Manually)**:
   - Drag **GetFile** and **PutHDFS** processors onto the canvas for each of the 3 data streams:
     - **Usage stream**:
       - `GetFile`: Input Directory = `/home/student/nifi_inbox`, File Filter = `usage_.*\.jsonl`, Keep Source File = `false`
       - `PutHDFS`: Directory = `/user/student/churn/raw/usage_json`, Hadoop Config Resources = `/home/hadoop/hadoop/etc/hadoop/core-site.xml,/home/hadoop/hadoop/etc/hadoop/hdfs-site.xml`, Conflict Resolution = `replace`
     - **Offers stream**:
       - `GetFile`: Input Directory = `/home/student/nifi_inbox`, File Filter = `offers\.csv`, Keep Source File = `false`
       - `PutHDFS`: Directory = `/user/student/churn/raw/offers`, Conflict Resolution = `replace`
     - **Tickets stream**:
       - `GetFile`: Input Directory = `/home/student/nifi_inbox`, File Filter = `support_tickets.*\.csv`, Keep Source File = `false`
       - `PutHDFS`: Directory = `/user/student/churn/raw/support_tickets`, Conflict Resolution = `replace`

4. Connect `GetFile` $\rightarrow$ `PutHDFS` on `success` relationship for each pair.
5. In `PutHDFS` processor properties, set `failure` and `success` to auto-terminate (or route failure to retry loop).

---

## 3. Running and Capturing Screenshots

1. Click **Start** on the Process Group or toolbar to run all processors.
2. Observe the flowfiles moving through the queues into HDFS.
3. **Take a screenshot of the running NiFi canvas** with active queues/metrics and save it to `screenshots/nifi_canvas.png`.

---

## 4. Verification in HDFS

Verify that all files have landed in HDFS:

```bash
hdfs dfs -ls -R /user/student/churn/raw
```

Expected directories:
- `/user/student/churn/raw/customers` (populated by Sqoop)
- `/user/student/churn/raw/support_tickets`
- `/user/student/churn/raw/offers`
- `/user/student/churn/raw/usage_json`

---

## 5. Automated Streaming Fallback (If NiFi is Headless / Unavailable)

If NiFi UI is not accessible or if running headless in automated tests, you can stage data directly into HDFS using the following fallback script:

```bash
# Fallback ingestion script:
hdfs dfs -mkdir -p /user/student/churn/raw/{support_tickets,offers,usage_json}
hdfs dfs -put -f data/support_tickets_rekeyed.csv /user/student/churn/raw/support_tickets/
hdfs dfs -put -f data/offers.csv /user/student/churn/raw/offers/

# Simulate streaming usage chunks
for f in ~/nifi_inbox/usage_*.jsonl; do
    hdfs dfs -put -f "$f" /user/student/churn/raw/usage_json/
done
```
