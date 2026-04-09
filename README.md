# 🚖 Metadata-Driven Lakehouse Architecture (Real-Time Streaming) 

[![Azure](https://img.shields.io/badge/Azure-0089D6?style=for-the-badge&logo=microsoft-azure&logoColor=white)]()
[![Databricks](https://img.shields.io/badge/Databricks-FF3621?style=for-the-badge&logo=databricks&logoColor=white)]()
[![Apache Spark](https://img.shields.io/badge/Apache_Spark-E25A1C?style=for-the-badge&logo=apache-spark&logoColor=white)]()
[![Python](https://img.shields.io/badge/Python-3776AB?style=for-the-badge&logo=python&logoColor=white)]()

> **🎬 Live Demonstration:** Watch the full [2-Hour Live Technical Walkthrough & SCD Demo Here]([Insert YouTube Link])

## 📌 Project Overview
This repository contains the code and architectural blueprints for an end-to-end, real-time streaming Data Lakehouse built natively on Microsoft Azure. The project simulates a ride-hailing data ecosystem, handling high-velocity live events alongside historical batch data. 

The primary goal of this project is to demonstrate enterprise-grade data engineering practices, including **Medallion Architecture**, **Slowly Changing Dimensions (SCD Type 2)**, metadata-driven pipelines via **Jinja Templating**, and strict data governance using **Databricks Unity Catalog**.

---

## 🏗️ Architecture Diagram

<img width="1536" height="1024" alt="Architecture 2 0" src="https://github.com/user-attachments/assets/17f3e54b-d6e3-4e0e-b1d9-3ae0d94836d3" />

---

## 🛠️ Tech Stack & Tools
* **Data Ingestion:** Azure Data Factory (ADF), Azure Event Hubs (Kafka Pub-Sub)
* **Storage:** Azure Data Lake Storage Gen2 (ADLS Gen2)
* **Compute & Processing:** Azure Databricks (Premium), PySpark, Delta Lake
* **Pipeline Orchestration:** Databricks Workflows, Delta Live Tables (DLT)
* **Data Governance & Security:** Unity Catalog (RBAC), Databricks Access Connectors
* **Configuration & Logic:** Jinja2 Templating
* **Analytics Serving:** Power BI via Databricks Partner Connect

---

## 🚀 Key Features

### 1. Hybrid Data Ingestion (Batch + Streaming)
* **Historical Data:** Parameterized Azure Data Factory pipelines execute batch pulls to land bulk datasets into ADLS Gen2.
* **Live Streaming:** A custom Python web app simulates ride bookings, pushing JSON payloads to Azure Event Hubs, acting as a high-throughput ingestion layer.

### 2. Metadata-Driven Transformations (Silver Layer)
Instead of hardcoding transformation logic, the pipeline utilizes **Jinja templates**. This dynamically fetches static mapping files and schema definitions directly from GitHub, decoupling the logic from the executing code and making the pipeline highly extensible.

<img width="1683" height="1015" alt="Obt py code" src="https://github.com/user-attachments/assets/986f6cc9-efbe-4351-b54c-7147b6e35acd" />

### 3. SCD Type 2 Implementation
Historical data integrity is preserved using Slowly Changing Dimensions (SCD Type 1 & 2) in the Silver layer. When a driver's state changes (e.g., upgrading their vehicle tier), the pipeline dynamically updates the active flags and effective dates without dropping the historical record.

<img width="1613" height="1071" alt="SCD Type 2 Output" src="https://github.com/user-attachments/assets/36f91d1d-397b-426f-90b9-c735de09c968" />

### 4. The One Big Table (OBT) & DLT
The Gold layer denormalizes the cleansed fact and dimension tables into a highly optimized One Big Table (OBT) using **Delta Live Tables (DLT)**. Strict data quality checks (`@dlt.expect_or_drop`) are enforced to ensure business-ready datasets are served to downstream BI teams with significantly reduced query latency.

---

## ⚙️ Pipeline Execution Flow

1. **Producer Trigger:** The Python simulator begins pushing ride events to Event Hubs.
2. **Bronze Ingestion:** Databricks Auto Loader reads the raw stream and batch data, landing it into the Bronze Delta tables.
3. **Silver Cleansing:** Data is cleansed, deduplicated, and conformed. SCD logic is applied to dimension tables. 
4. **Gold Aggregation:** DLT pipelines execute, joining facts and dimensions via Jinja mappings to populate the OBT.
5. **BI Serving:** Power BI connects directly to the Gold OBT via Partner Connect for real-time dashboarding.

<img width="1916" height="1078" alt="DAG Graph ubber_end_to_end_orchestration" src="https://github.com/user-attachments/assets/49970600-d308-4d98-8e28-118e77ef16e0" />

<img width="1913" height="1078" alt="DAG for uber_rides_ingest pipeline " src="https://github.com/user-attachments/assets/0f32d76f-1f12-4c1e-9405-24cde2734e64" />

---

## 🚧 Challenges & Solutions

* **Challenge:** Handling schema evolution and historical state changes in a live stream without halting the pipeline.
  * **Solution:** Engineered robust `MERGE` logic within the PySpark streaming micro-batches to gracefully handle SCD Type 2 inserts and updates on the fly.
* **Challenge:** Preventing the pipeline from becoming rigid and tightly coupled to the initial dataset.
  * **Solution:** Introduced Jinja templating to parameterize configurations, allowing the pipeline to adapt to new dimension mappings without requiring core code changes.

---

## 🤝 Connect with Me
I am a Data Engineer actively looking for new opportunities. If you're building data-driven solutions and looking for someone with hands-on architectural experience, let's connect!

* **LinkedIn:** [https://www.linkedin.com/in/nafis-ansari-63878b182/]
* **Portfolio:** [https://nafisansari786.github.io/Nafis.github.io/#]
* **Email:** nafisansari786@outlook.com
