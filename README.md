# Fraud streamhouse with Fluss, Flink and Iceberg


Disclaimer:<br>
I created this repository to experiment with Apache Fluss (Incubating). Please note that the cluster sizing, security configuration, fraud detection logic are intended for exploration only and are not production-ready.<br><br>

---

<img width="845" height="380" alt="flussArch" src="https://github.com/user-attachments/assets/b8a2dfa6-9f7c-4dfb-9343-69e14de27a27" />

---

# Apache Fluss: The Game Changer in Data Streaming (Part 2)

Following my previous post introducing **Fluss**, here’s how I built a **Streamhouse architecture** leveraging **Fluss**, **Flink**, and **Iceberg** to:

- Process bank transactions in real time
- Detect fraud
- Serve data seamlessly across **hot (sub-second latency)** and **cold (minutes latency)** layers

[Linkedin post](https://www.linkedin.com/posts/activity-7402102138476052480-Xe8q?utm_source=share&utm_medium=member_desktop&rcm=ACoAAD71668BPwA1sMmdXJo2b8_gg_eDOtgR-Ww)

---

## 🚀 Deployment Setup
Using **Docker Compose**, I deployed a small Fluss and Flink cluster.  
I chose **MinIO** as the S3-compatible storage to hold Iceberg tables, and an **Iceberg REST Catalog** for metadata management.

---

## 🔹 Fluss Initialization & Data Generation
Fluss Java Client creates three tables:

1. **Transaction Log Table** (append-only)
2. **Account Primary Table** (supports `INSERT`, `UPDATE`, `DELETE`)
3. **Enriched Fraud Log Table** with `datalake` option enabled (append-only)

When `datalake` option is enabled, Fluss automatically initializes an **Iceberg table** on MinIO through the REST Catalog for historical data.  
Finally, the client generates transaction and account records and pushes them to Fluss.

---

## 🔍 Fraud Detection & Enrichment
The **Flink-based fraud detection job**, developed to continuously stream transactions from Fluss, identifies fraudulent records in real time.

- Once fraud is detected, the records are **enriched with the account name** by referencing the Account Primary Table.
- This enrichment uses a **temporal streaming lookup join** on the Account table, which serves as a dimension.
- Finally, the enriched fraud records are appended to the **Enriched Fraud Log Table**.

---

## 🏗 Tiering to Lakehouse
The **Fluss Tiering Service** (a Flink Job provided by the Fluss ecosystem) appends and compacts enriched fraud into the **Iceberg table on MinIO**.

---

## ✅ Why I Consider Apache Fluss (Incubating) a True Game Changer in Data Streaming

- **Queryable Tables**  
  Unlike Apache Kafka, where topics are not queryable, Fluss Log Tables allow direct querying for real-time insights.

- **No More External Caches**  
  Eliminate the need to deploy/scale cache/DB/state for lookups—just use Fluss Primary Tables.

- **Column Pruning for Streaming Reads**  
  Fluss supports column pruning for Log Tables and Primary Key Table changelogs, reducing data reads and network costs—even during streaming reads.

- **Automatic Tiering to Lakehouse**  
  Real-time data is automatically compacted into Iceberg, Paimon, or Lance via a built-in Flink service, seamlessly bridging streaming and batch.

- **Union Reads**  
  Fluss enables combined reads of real-time and historical data (Fluss tables + Iceberg), delivering true real-time analytics without duplication.

- **Cut Disk Storage Costs**  
  Fluss offloads old data (Log Table segments and Primary Table snapshots) to remote storage, significantly reducing disk costs.

---

## 🔹 CMDs

Fluss/Flik required JARs:
`https://fluss.apache.org/docs/streaming-lakehouse/integrate-data-lakes/iceberg/`


Fraud Job JAR packaging cmd:
```bash
mvn package -f pom.xml
```


Flink Job submit parameters:
```bash
--parallelism 2 --fluss.bootstrap.servers coordinator-server:9122 --datalake.format iceberg --datalake.iceberg.type rest --datalake.iceberg.warehouse s3://fluss/data --datalake.iceberg.uri http://rest:8181 --datalake.iceberg.s3.endpoint http://minio:9000 --datalake.iceberg.s3.path-style-access true
```


Docker commands:
```bash
docker-compose down -v
docker-compose up --build
```


Flink SQL client commands:
```sql
CREATE CATALOG fluss_catalog WITH (
    'type' = 'fluss',   
    'bootstrap.servers' = 'coordinator-server:9122'
);

USE CATALOG fluss_catalog;

SET 'execution.runtime-mode' = 'batch';

SET 'sql-client.execution.result-mode' = 'tableau';

select * from fraud;

select * from fraud$lake;
```
