# ☁️ Google Cloud Certifications & Core Data Services

## 🎓 Certifications

- ✅ [**Google Cloud Certified: Professional Data Engineer**](https://www.credly.com/badges/8b12f9ce-eca0-4704-a446-3c1ee86f0c1a/public_url)  
  _Demonstrates deep knowledge in designing, building, maintaining, and optimizing data processing systems._

- ✅ [**Google Cloud Certified: Cloud Digital Leader**](https://www.credly.com/badges/5a781c4a-f5fd-4322-a0ac-658bf9c960b0/public_url)  
  _Validates foundational knowledge of cloud technologies and Google Cloud services._

---

## 📌 Google Cloud Core Data Services

A comprehensive overview of GCP services aligned with the **Google Cloud Professional Data Engineer** exam guide. This breakdown follows the typical data pipeline lifecycle—from ingestion to processing, storage, analysis, and beyond.

---

### 🔄 Data Ingestion

- **Cloud Pub/Sub**  
  Real-time messaging service for ingesting event streams at scale. Enables decoupled and asynchronous data ingestion for microservices and streaming analytics.

- **Dataflow (Streaming Mode)**  
  Unified stream processing using Apache Beam for transforming and enriching data in motion.

- **BigQuery Data Transfer Service (BQ DTS)**  
  Scheduled, serverless service to transfer data from Google SaaS (e.g., Google Ads, YouTube) and external sources into BigQuery.

- **Storage Transfer Service**  
  Transfer large-scale datasets from on-prem or cloud storage platforms into Cloud Storage.

- **Cloud IoT Core** *(now retired)*  
  Was used to securely ingest data from IoT devices.

---

### 📦 Data Storage

- **Cloud Storage**  
  Object storage for binary, semi-structured, and unstructured data. Offers multiple storage classes based on access frequency and cost.

- **BigQuery**  
  Columnar, serverless data warehouse designed for petabyte-scale analytical workloads. Ideal for long-term analytical storage.

- **Cloud Spanner**  
  Globally distributed relational database with horizontal scalability, strong consistency, and SQL support.

- **Cloud SQL**  
  Managed MySQL, PostgreSQL, and SQL Server for transactional workloads with compatibility and ease of use.

- **Cloud Firestore**  
  Serverless, NoSQL document database for high-availability mobile/web apps.

- **Cloud Bigtable**  
  Wide-column, low-latency NoSQL database optimized for IoT, finance, and time-series data.

---

### ⚙️ Data Processing

- **Dataflow (Batch + Streaming)**  
  Serverless data processing using Apache Beam for ETL, real-time analytics, and ML pipelines.

- **Dataproc**  
  Managed Spark, Hadoop, and Hive clusters for scalable batch data processing with native ecosystem integration.

- **BigQuery**  
  Also serves as a processing engine for analytics via SQL (ELT model).

- **Dataprep by Trifacta**  
  Visual data preparation tool for cleaning and transforming data with minimal code.

- **Vertex AI Pipelines**  
  For training and serving ML models as part of data workflows.

---

### 🔄 Orchestration & Automation

- **Cloud Composer**  
  Managed Apache Airflow for authoring, scheduling, and monitoring complex workflows.

- **Workflows**  
  Serverless orchestration to connect services and APIs with step-by-step logic.

- **Cloud Functions / Cloud Run**  
  Lightweight compute services for triggering transformations or pipeline steps.

---

### 📊 Data Analysis

- **BigQuery**  
  Powerful SQL-based analytics at scale; supports ML, geospatial, BI Engine acceleration, and federated queries.

- **Looker / Looker Studio**  
  Modern business intelligence platforms for interactive dashboards and self-service data exploration.

- **Vertex AI**  
  Unified ML platform for model building, training, tuning, deployment, and monitoring.

- **AutoML**  
  No-code ML model creation for vision, NLP, tabular data, etc.

---

### 📈 Visualization

- **Looker / Looker Studio**  
  - Drag-and-drop dashboarding tools for real-time reporting.  
  - Integrates natively with BigQuery and other data sources.

- **Data Studio (now part of Looker)**  
  Easy-to-use, no-code dashboard tool for business users.

---

### 🔍 Monitoring & Logging

- **Cloud Logging (formerly Stackdriver Logging)**  
  Collects logs from GCP resources, applications, and services.

- **Cloud Monitoring**  
  Provides visibility into performance metrics, uptime, and system health.

- **Error Reporting & Debugger**  
  Useful for monitoring application reliability and identifying runtime exceptions.

---

### 🔐 Security & Governance

- **IAM (Identity and Access Management)**  
  Fine-grained access control for services, roles, and resources.

- **VPC Service Controls**  
  Adds an additional security perimeter around sensitive data services like BigQuery, Cloud Storage, etc.

- **Cloud Key Management Service (KMS)**  
  Centralized key management for encrypting data.

- **Data Loss Prevention (DLP)**  
  Automatically detects and redacts sensitive PII/PCI/PHI data across structured and unstructured datasets.

- **Cloud Audit Logs**  
  Provides a detailed record of every interaction with GCP resources for compliance and auditing.

---

## ✅ Summary

This layered service architecture enables Data Engineers to:

- Ingest, store, and process vast amounts of data.
- Operationalize machine learning and analytical models.
- Build secure, scalable, and governed cloud-native data systems.
- Drive business intelligence and informed decision-making.

> 📌 _These tools are essential for designing and maintaining data systems, as outlined in the GCP Professional Data Engineer certification exam guide._

