# 🧪 Clinical Trials vs Real-World Drug Safety Analytics Pipeline  
### (AACT + FAERS)

🎯 **End-to-end data engineering and analytics solution to evaluate gaps between clinical trial safety and real-world drug outcomes**

This project implements a production-grade ETL and analytics pipeline that integrates **ClinicalTrials.gov (AACT)** data with **FDA FAERS** post-approval adverse event data.  
The objective is to assess whether controlled clinical trials adequately represent real-world drug safety risks observed after market approval.

---

## 🛠️ Tech Stack
- **Databricks** – Cloud-based data engineering platform  
- **Apache Spark (PySpark)** – Distributed data processing and transformations  
- **Delta Lake** – ACID transactions, data versioning, and scalable storage  
- **SQL** – Analytical querying and aggregations  
- **Python** – Data processing and orchestration  
- **Power BI** – Interactive dashboards and data storytelling  

---

## 🏗️ Architecture

### Medallion Architecture
- **Bronze** – Raw ingestion with audit columns and traceability  
- **Silver** – Cleaned, standardized, and deduplicated datasets  
- **Gold** – Star-schema analytics layer optimized for BI  

### Dimensional Modeling
- **Fact Tables**
  - Clinical Trials (AACT)
  - Adverse Events (FAERS)
- **Dimension Tables**
  - Drugs
  - Conditions
  - Patients
  - Studies
  - Dates  

Designed for high-performance analytical queries and BI consumption.

---

## 📊 Key Metrics
- Total Clinical Trials (AACT)  
- Average Trial Enrollment  
- Total FAERS Adverse Events  
- Serious, Hospitalization, and Death Events  
- Clinical Trial Coverage vs Real-World Safety Gap  

---

## 🔍 Key Insights
- Clinical trials evaluate drugs on limited and controlled populations, while FAERS captures safety outcomes from large-scale real-world usage.
- Significant volumes of serious adverse events emerge post-approval, despite drugs completing clinical trials.
- Trial condition coverage is narrow, whereas real-world usage includes broader and off-label conditions.
- Short trial durations fail to capture long-term and delayed adverse effects observed in FAERS data.
- Safety gaps vary significantly by drug, indicating that risk is drug-specific rather than uniform across approvals.
- Demographic factors such as age and sex influence adverse outcomes, highlighting differences between trial populations and real-world patients.

---

## ⭐ Key Features
- **Integrated Clinical + Real-World Safety Analysis**  
  Combines AACT (expected safety) and FAERS (actual outcomes) into a unified analytical model.

- **Production-Grade Data Pipeline**  
  Implements audit logging, idempotent ingestion, schema enforcement, and error handling.

- **Star Schema for BI Optimization**  
  Clean dimensional model designed for fast Power BI reporting and slicing.

- **Safety Gap Analytics**  
  Quantifies differences between clinical trial enrollment, condition coverage, and real-world adverse events.

- **Regulatory-Aware Design**  
  Uses FAERS appropriately for signal detection without making causal claims.

- **Scalable & Extensible Framework**  
  Easily extendable for trend analysis, demographic stratification, and drug class comparisons.

---

## 📈 Dashboards
- **Clinical Trial Safety Overview (AACT)**  
  Trial phases, enrollment, duration, and condition coverage  

- **Real-World Drug Safety Overview (FAERS)**  
  Serious outcomes, hospitalizations, and deaths  

- **Clinical vs Real-World Safety Gap**  
  Expected safety vs actual post-approval outcomes  

---

## 🧠 Business Value
This project demonstrates how post-approval real-world evidence can reveal drug safety risks not visible during clinical trials.  
It supports improved **pharmacovigilance**, **regulatory monitoring**, and **data-driven healthcare decision-making** by bridging the gap between controlled clinical research and real-world outcomes.

---

## 👩‍💻 Author
**Buvanaa Ravi**  
Graduation Project – Data Engineering

