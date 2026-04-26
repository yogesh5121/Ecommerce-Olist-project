Ecommerce Olist — Enterprise-Grade Azure ELT Pipeline
📌 Project Overview
An enterprise-grade, cloud-native ELT pipeline built on Microsoft Azure to ingest data from 6 heterogeneous sources into a unified data lakehouse — following the Medallion Architecture (Bronze → Silver → Gold).
The pipeline handles multi-source ingestion via Azure Data Factory, stores raw data in ADLS Gen2 as Parquet, processes it in Azure Databricks via Unity Catalog, and implements SCD Type 1 & SCD Type 2 using PySpark — delivering analytics-ready Delta Lake tables.

Architecture
  ┌──────────────────────────────────────────────────────────┐
  │                  DATA SOURCES (6 Types)                  │
  │  CSV | Excel | SFTP | Snowflake | On-Prem SQL | REST API │
  └───────────────────────┬──────────────────────────────────┘
                          │
                          ▼
  ┌───────────────────────────────────────────────────────────┐
  │                Azure Data Factory (ADF)                   │
  │                                                           │
  │  ┌──────────┐ ┌──────────┐ ┌──────────┐ ┌────────────┐  │
  │  │CSV Pipe  │ │SFTP Pipe │ │ API Pipe │ │Excel Pipe  │  │
  │  └────┬─────┘ └────┬─────┘ └────┬─────┘ └─────┬──────┘  │
  │       │            │            │              │          │
  │  ┌────┴─────┐ ┌────┴──────────────────────────┴──────┐   │
  │  │SQL Pipe  │ │         MASTER PIPELINE               │   │
  │  └────┬─────┘ │  (Orchestrates all child pipelines)  │   │
  │       └───────►  Trigger-based scheduling per source  │   │
  │               └───────────────────┬───────────────────┘   │
  └───────────────────────────────────┼───────────────────────┘
                                      │ Parquet Format
                                      ▼
  ┌───────────────────────────────────────────────────────────┐
  │                 ADLS Gen2 — Bronze Layer                  │
  │            Raw Parquet files per source system            │
  └───────────────────────────┬───────────────────────────────┘
                              │ Access Connector
                              ▼ Unity Catalog
  ┌───────────────────────────────────────────────────────────┐
  │                   Azure Databricks                        │
  │                                                           │
  │   Silver Layer → Cleaned, typed, deduplicated data        │
  │   Gold Layer   → SCD Type 1 & SCD Type 2 Delta tables     │
  └───────────────────────────┬───────────────────────────────┘
                              │
                              ▼
                     BI / Reporting / ML
Medallion Architecture
🥉 BronzeParquetRaw data landed from all 6 sources via ADF — no transformations
🥈 SilverDeltaCleaned, typed, deduplicated data using PySpark
🥇 GoldDeltaSCD1 & SCD2 tables — analytics-ready for BI and reporting

Data Sources:

Source           TypeIngestion          Method
CSV Files         Flat file            ADF Copy Activity
Excel Files       Flat file            ADF Copy Activity
Json              json file            ADF Copy Activity
On-Premises SQL   Relational DB        ADF Self-hosted Integration Runtime
REST API          Web service          ADF Web / REST Connector
Snowflake         Cloud Data Warehouse ADF Snowflake Connector

Tech Stack:

Tool                                 Purpose
Azure Data Factory (ADF)     Multi-source ingestion & pipeline orchestration
ADLS Gen2                    Scalable data lake storage (Parquet format)
Azure Databricks             Distributed PySpark processing
Unity Catalog                Centralized data governance & access control
PySpark                      Data transformation & SCD implementation
Delta Lake                   ACID-compliant Gold layer tables
Medallion Architecture       Bronze → Silver → Gold layered data design

Pipeline Design
1.Child Pipelines — Per Source
   Individual ADF pipelines created for each source system:
   Pipeline                      Source
   pl_ingest_csv              Flat CSV files
   pl_ingest_excel            Excel files
   pl_ingest_sftp             SFTP server
   pl_ingest_snowflake        Snowflake cloud DW
   pl_ingest_onprem_sql       On-premises SQL Server (Self-hosted IR)
   pl_ingest_rest_api         REST API endpoints

Master Pipeline

Orchestrates all child pipelines using Execute Pipeline activity
Trigger-based scheduling — each source has its own trigger (Schedule / Event / Tumbling Window)
All data lands in ADLS Gen2 in Parquet format under the Bronze layer

Silver Layer — Databricks + Unity Catalog

ADLS Gen2 connected to Databricks via Access Connector
Data governed and accessed through Unity Catalog
PySpark transformations applied:

Null handling & deduplication
Data type casting & schema standardization
Business rule validations

Output written as managed Delta tables in Unity Catalog

Gold Layer — SCD Implementation
SCD Type 1 — Overwrite (No History)
Used for dimensions where only the latest value matters:
deltaTable.alias("target").merge(
    updates.alias("source"),
    "target.id = source.id"
).whenMatchedUpdateAll() \
 .whenNotMatchedInsertAll() \
 .execute()

SCD Type 2 — History Preserved
Used for dimensions where historical changes must be tracked:
deltaTable.alias("target").merge(
    updates.alias("source"),
    "target.id = source.id AND target.is_current = true AND source.hash != target.hash"
).whenMatchedUpdate(set={
    "is_current": lit(False),
    "end_date": current_date()
}).execute()

# Step 2: Insert new/changed records
updates.withColumn("is_current", lit(True)) \
       .withColumn("start_date", current_date()) \
       .withColumn("end_date", lit(None)) \
       .write.format("delta").mode("append").saveAsTable("gold.dim_table")

Key Outcomes

✅ Unified 6 heterogeneous data sources into a single lakehouse
✅ Parquet format for cost-efficient storage and fast reads
✅ SCD Type 1 & 2 for accurate, historically-tracked dimensional modeling
✅ Unity Catalog for governed, secure, role-based data access
✅ Analytics-ready Delta tables delivered to BI teams
