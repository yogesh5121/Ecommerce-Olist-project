<div align="center">

# 🛒 Ecommerce Olist — Enterprise Azure ELT Pipeline

<p>
  <img src="https://img.shields.io/badge/Azure%20Data%20Factory-0078D4?style=for-the-badge&logo=microsoftazure&logoColor=white"/>
  <img src="https://img.shields.io/badge/Azure%20Databricks-FF3621?style=for-the-badge&logo=databricks&logoColor=white"/>
  <img src="https://img.shields.io/badge/ADLS%20Gen2-0089D6?style=for-the-badge&logo=microsoftazure&logoColor=white"/>
  <img src="https://img.shields.io/badge/Delta%20Lake-00ADD8?style=for-the-badge&logo=databricks&logoColor=white"/>
  <img src="https://img.shields.io/badge/PySpark-E25A1C?style=for-the-badge&logo=apachespark&logoColor=white"/>
  <img src="https://img.shields.io/badge/Snowflake-29B5E8?style=for-the-badge&logo=snowflake&logoColor=white"/>
  <img src="https://img.shields.io/badge/Unity%20Catalog-FF3621?style=for-the-badge&logo=databricks&logoColor=white"/>
</p>

<p>
  <img src="https://img.shields.io/badge/Status-Completed-brightgreen?style=flat-square"/>
  <img src="https://img.shields.io/badge/Architecture-Medallion-gold?style=flat-square"/>
  <img src="https://img.shields.io/badge/Sources-6%20Heterogeneous-blue?style=flat-square"/>
  <img src="https://img.shields.io/badge/SCD-Type%201%20%26%202-purple?style=flat-square"/>
</p>

<br/>

> **End-to-end cloud-native ELT pipeline** ingesting data from 6 heterogeneous sources into a unified Azure Data Lakehouse using Medallion Architecture (Bronze → Silver → Gold) with SCD Type 1 & 2 implementation.

</div>

---

## 📋 Table of Contents

- [Overview](#-overview)
- [Architecture](#-architecture)
- [Data Sources](#-data-sources)
- [Tech Stack](#-tech-stack)
- [Pipeline Design](#-pipeline-design)
- [Medallion Architecture](#-medallion-architecture)
- [SCD Implementation](#-scd-implementation)
- [Repository Structure](#-repository-structure)
- [Getting Started](#-getting-started)
- [Key Outcomes](#-key-outcomes)
- [Author](#-author)

---

## 🔍 Overview

This project simulates a **real-world enterprise data engineering workflow** on Microsoft Azure. It ingests data from **6 different source systems** — CSV, Excel, JSON, SFTP, On-Premises SQL, REST API, and Snowflake — unifies them in **ADLS Gen2 as Parquet**, transforms them in **Azure Databricks** via **Unity Catalog**, and delivers analytics-ready **Delta Lake tables** with full **SCD Type 1 & Type 2** support.

**What makes this project enterprise-grade:**
- 🔀 Multi-source ingestion with dedicated pipelines per source
- 🎯 Master pipeline orchestrating all child pipelines with trigger-based scheduling
- 🏛️ Unity Catalog for centralized governance and access control
- 🔄 SCD1 & SCD2 for accurate dimensional modeling with history tracking
- ⚡ Delta Lake for ACID-compliant, high-performance analytical tables

---

## 🏗️ Architecture

```
╔══════════════════════════════════════════════════════════════╗
║                    DATA SOURCES (6 Types)                    ║
║    CSV  │  Excel  │  JSON  │  SFTP  │  On-Prem SQL  │  API   ║
║                      + Snowflake                             ║
╚══════════════════════════════╦═══════════════════════════════╝
                               ║
                               ▼
╔══════════════════════════════════════════════════════════════╗
║                  AZURE DATA FACTORY (ADF)                    ║
║                                                              ║
║  ┌──────────┐ ┌──────────┐ ┌──────────┐ ┌──────────────┐   ║
║  │ pl_csv   │ │ pl_excel │ │ pl_sftp  │ │  pl_rest_api │   ║
║  └────┬─────┘ └────┬─────┘ └────┬─────┘ └──────┬───────┘   ║
║       │            │            │               │            ║
║  ┌────┴─────┐ ┌────┴──────────────────────────── ┴──────┐   ║
║  │pl_sql_ir │ │           MASTER PIPELINE               │   ║
║  └────┬─────┘ │   Execute Pipeline │ Triggers per src   │   ║
║       └──────►│   Schedule │ Event │ Tumbling Window    │   ║
║               └──────────────────┬──────────────────────┘   ║
╚══════════════════════════════════╬═══════════════════════════╝
                                   ║  Sink: Parquet Format
                                   ▼
╔══════════════════════════════════════════════════════════════╗
║              ADLS GEN2 — 🥉 BRONZE LAYER                    ║
║         Raw Parquet files partitioned per source             ║
╚══════════════════════════════════╦═══════════════════════════╝
                                   ║  Access Connector
                                   ║  Unity Catalog
                                   ▼
╔══════════════════════════════════════════════════════════════╗
║                    AZURE DATABRICKS                          ║
║                                                              ║
║  🥈 SILVER  →  Cleaned │ Typed │ Deduplicated (PySpark)     ║
║                                                              ║
║  🥇 GOLD    →  SCD Type 1 & SCD Type 2 (Delta Tables)       ║
╚══════════════════════════════════╦═══════════════════════════╝
                                   ║
                                   ▼
                        BI  │  Reporting  │  ML
```

---

## 📥 Data Sources

| # | Source | Type | ADF Connector |
|---|--------|------|---------------|
| 1 | **CSV Files** | Flat file | Copy Activity |
| 2 | **Excel Files** | Flat file | Copy Activity |
| 3 | **JSON Files** | Semi-structured | Copy Activity |
| 4 | **SFTP Server** | File transfer | SFTP Connector |
| 5 | **On-Premises SQL** | Relational DB | Self-hosted Integration Runtime |
| 6 | **REST API** | Web service | Web / REST Connector |
| 7 | **Snowflake** | Cloud Data Warehouse | Snowflake Connector |

---

## ⚙️ Tech Stack

| Layer | Tool | Role |
|-------|------|------|
| **Ingestion** | Azure Data Factory | Multi-source orchestration & copy |
| **Storage** | ADLS Gen2 | Data lake (Parquet → Bronze) |
| **Processing** | Azure Databricks | Distributed PySpark compute |
| **Governance** | Unity Catalog | Access control & data lineage |
| **Connectivity** | Access Connector | Secure ADLS ↔ Databricks link |
| **Transform** | PySpark | Cleaning, joins, SCD logic |
| **Table Format** | Delta Lake | ACID transactions, time travel |
| **Pattern** | Medallion Architecture | Bronze → Silver → Gold layering |

---

## 🔄 Pipeline Design

### Child Pipelines — One Per Source

Each source system has its own dedicated ADF pipeline for isolation, maintainability, and independent scheduling:

```
pl_ingest_csv          →  Ingest flat CSV files
pl_ingest_excel        →  Ingest Excel workbooks
pl_ingest_json         →  Ingest JSON files
pl_ingest_sftp         →  Pull files from SFTP server
pl_ingest_onprem_sql   →  On-prem SQL via Self-hosted IR
pl_ingest_rest_api     →  Call REST API endpoints
pl_ingest_snowflake    →  Extract from Snowflake DW
```

### Master Pipeline — Orchestration Layer

```
Master Pipeline
    │
    ├── Execute Pipeline → pl_ingest_csv          (Schedule Trigger)
    ├── Execute Pipeline → pl_ingest_excel         (Schedule Trigger)
    ├── Execute Pipeline → pl_ingest_json          (Schedule Trigger)
    ├── Execute Pipeline → pl_ingest_sftp          (Tumbling Window)
    ├── Execute Pipeline → pl_ingest_onprem_sql    (Schedule Trigger)
    ├── Execute Pipeline → pl_ingest_rest_api      (Event Trigger)
    └── Execute Pipeline → pl_ingest_snowflake     (Schedule Trigger)
```

> All pipelines sink data into **ADLS Gen2 in Parquet format** under the Bronze container.

---

## 🧱 Medallion Architecture

### 🥉 Bronze — Raw Layer
- Data lands **as-is** from source systems
- Stored as **Parquet** files in ADLS Gen2
- No transformations — preserves source fidelity
- Partitioned by source system and ingestion date

### 🥈 Silver — Cleansed Layer
- Connected via **Access Connector + Unity Catalog**
- PySpark transformations:
  - ✅ Null handling & deduplication
  - ✅ Data type casting & schema enforcement
  - ✅ Column renaming & standardization
  - ✅ Business rule validations
- Written as **managed Delta tables** in Unity Catalog

### 🥇 Gold — Analytical Layer
- Dimensional model with **SCD Type 1 & SCD Type 2**
- Optimized Delta tables for BI consumption
- Governed via Unity Catalog with role-based access

---

## 🔄 SCD Implementation

### SCD Type 1 — Overwrite (No History Required)

Used for dimensions where only the **current value** matters (e.g., product descriptions, customer contact info):

```python
from delta.tables import DeltaTable

deltaTable = DeltaTable.forName(spark, "gold.dim_customers")

deltaTable.alias("target").merge(
    updates.alias("source"),
    "target.customer_id = source.customer_id"
).whenMatchedUpdateAll() \
 .whenNotMatchedInsertAll() \
 .execute()
```

### SCD Type 2 — History Preserved

Used for dimensions where **historical changes must be tracked** (e.g., customer address, product category):

```python
from delta.tables import DeltaTable
from pyspark.sql.functions import lit, current_date, md5, concat_ws

# Step 1: Generate hash to detect changes
updates = updates.withColumn(
    "hash", md5(concat_ws("||", *change_columns))
)

# Step 2: Expire changed existing records
deltaTable.alias("target").merge(
    updates.alias("source"),
    """target.id = source.id 
       AND target.is_current = true 
       AND source.hash != target.hash"""
).whenMatchedUpdate(set={
    "is_current":  lit(False),
    "end_date":    current_date()
}).execute()

# Step 3: Insert new & changed records as current
updates.withColumn("is_current",  lit(True)) \
       .withColumn("start_date",  current_date()) \
       .withColumn("end_date",    lit(None).cast("date")) \
       .write.format("delta") \
       .mode("append") \
       .saveAsTable("gold.dim_table")
```

**SCD2 Schema:**

| Column | Description |
|--------|-------------|
| `surrogate_key` | Auto-generated unique key per version |
| `natural_key` | Business key from source |
| `...attributes` | Tracked dimension columns |
| `hash` | MD5 of tracked columns for change detection |
| `is_current` | `true` = active record |
| `start_date` | When this version became active |
| `end_date` | When this version expired (`null` if current) |

---

## 📁 Repository Structure

```
Ecommerce-Olist-project/
│
├── 📂 ADF/
│   ├── 📂 pipeline/
│   │   ├── pl_ingest_csv.json
│   │   ├── pl_ingest_excel.json
│   │   ├── pl_ingest_json.json
│   │   ├── pl_ingest_sftp.json
│   │   ├── pl_ingest_onprem_sql.json
│   │   ├── pl_ingest_rest_api.json
│   │   ├── pl_ingest_snowflake.json
│   │   └── pl_master_pipeline.json
│   ├── 📂 linkedService/
│   ├── 📂 dataset/
│   └── 📂 trigger/
│
├── 📂 Databricks/
│   ├── 📂 silver/
│   │   └── silver_transformations.ipynb
│   └── 📂 gold/
│       ├── scd1_implementation.ipynb
│       └── scd2_implementation.ipynb
│
└── README.md
```

---

## 🚀 Getting Started

### Prerequisites

- [ ] Azure Subscription
- [ ] Azure Data Factory instance
- [ ] Self-hosted Integration Runtime (for On-Prem SQL)
- [ ] Azure Databricks workspace with Unity Catalog enabled
- [ ] ADLS Gen2 storage account with Access Connector configured
- [ ] Snowflake account with connection credentials

### Setup Steps

**1. Clone the repository**
```bash
git clone https://github.com/yogesh5121/Ecommerce-Olist-project.git
cd Ecommerce-Olist-project
```

**2. Set up ADLS Gen2**
```
Create containers:
  ├── bronze/
  ├── silver/
  └── gold/
```
Configure the Access Connector and assign Storage Blob Data Contributor role.

**3. Import ADF Pipelines**
- Open ADF Studio → Author → Import ARM Template
- Import all JSONs from `/ADF/pipeline/`
- Update Linked Services with your credentials
- Activate triggers per pipeline

**4. Configure Unity Catalog in Databricks**
```sql
CREATE CATALOG olist;
CREATE SCHEMA olist.bronze;
CREATE SCHEMA olist.silver;
CREATE SCHEMA olist.gold;
```

**5. Run Databricks Notebooks in Order**
```
1. Databricks/silver/silver_transformations.ipynb
2. Databricks/gold/scd1_implementation.ipynb
3. Databricks/gold/scd2_implementation.ipynb
```

**6. Trigger the Master Pipeline**

Run manually from ADF Studio or activate scheduled/event-based triggers.

---

## ✅ Key Outcomes

| Outcome | Detail |
|---------|--------|
| 🔀 **Multi-source unification** | 6+ heterogeneous sources unified into one lakehouse |
| 📦 **Efficient storage** | Parquet format for fast reads & cost savings |
| 🏛️ **Governed access** | Unity Catalog with role-based permissions |
| 🔄 **History tracking** | SCD Type 2 preserves full dimensional history |
| ⚡ **ACID compliance** | Delta Lake ensures reliable, consistent data |
| 📊 **BI-ready output** | Gold layer tables ready for Power BI / Tableau |

---

## 👨‍💻 Author

<div align="center">

**Yogesh S**

*Azure Data Engineer | 3 Years Experience*

`ADF` · `Databricks` · `PySpark` · `ADLS Gen2` · `Delta Lake` · `Unity Catalog`

[![LinkedIn](https://img.shields.io/badge/LinkedIn-Connect-0A66C2?style=for-the-badge&logo=linkedin)](https://www.linkedin.com/in/yogesh-s005/)
[![GitHub](https://img.shields.io/badge/GitHub-Follow-181717?style=for-the-badge&logo=github)](https://github.com/yogesh5121)

</div>

---

<div align="center">

📄 Licensed under the [MIT License](LICENSE) · ⭐ Star this repo if you found it helpful!

</div>
