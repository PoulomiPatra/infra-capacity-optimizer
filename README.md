# infra-capacity-optimizer

# 🛠 Infra Capacity Optimizer

A scalable Data Engineering pipeline built using **PySpark** and **Databricks** to ingest, process, analyze, and optimize simulated infrastructure health metrics (CPU, memory, storage). This project demonstrates raw-to-gold ETL design, risk detection, and operational insights for capacity forecasting.

---

##  Project Objective

Enable infrastructure teams to:
-  Detect overloaded or underutilized servers
-  Classify servers based on performance risk
-  Analyze CPU/Memory/Storage usage trends
-  Power dashboards with real-time metrics and alerts

---

##  Tech Stack

| Tool | Purpose |
|------|---------|
| **Databricks Community Edition** | Development & orchestration |
| **PySpark** | ETL transformations & logic |
| **Delta Lake** | Scalable data lake storage |
| **JSON** | Input data format |
| **DBFS** | File storage |
| **SQL** | Dashboard queries |

---

🛠 ETL Architecture

-  Bronze Layer:
Ingest raw JSON to Delta format

Stored the Flattened data with schema inference


-  Silver Layer:
 cleansed, and enriched metrics

Added derived fields: *_free_gb, *_free_percent

Tagged each server with a risk_level (High/Moderate/Low/Normal)


- Gold Layer:
Aggregated KPIs for dashboarding

Server classification: overloaded / underutilized

Trends by day

Alerts by severity

Real-time CPU/memory usage



