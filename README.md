# Data-Warehouse-Project
A complete end-to-end Modern Data Warehouse built on SQL Server using the Medallion Architecture (Bronze → Silver → Gold).
This project integrates ERP and CRM data, transforms it into a unified business model to enable business analytics.

## 1️⃣ Executive Summary
The company’s ERP and CRM data were siloed, causing inconsistent sales reporting and limiting visibility into customer and product performance.  
To address this, a Modern Data Warehouse was built using the Medallion Architecture (Bronze–Silver–Gold), integrating raw ERP/CRM files, applying data cleansing and standardization, and modeling a unified star schema for analytics.  
This solution significantly reduced manual reporting effort, improved data accuracy, and enabled consistent insights across teams. Future enhancements include adding historical tracking, automating ETL pipelines, and incorporating BI dashboards for interactive analysis.

## 2️⃣ Business Problem
The company’s ERP and CRM data were siloed and manually processed in spreadsheets. This caused:
- Duplicate and missing customer records
- Inconsistent product naming conventions
- Delays in sales reporting cycles
- Difficulty tracking performance across different markets

## 3️⃣ Methodology
<img width="800" height="590" alt="yuque_diagram" src="https://github.com/user-attachments/assets/44fb2b7e-5012-46b7-967d-6db6332b5019" />  

**Bronze Layer – Raw Data**
  - Load ERP&CRM CSV files
  - No transformations
  - Maintain full data lineage
**Silver Layer – Cleaned & Standardized**
  - Data type corrections
  - Missing value handling
  - Deduplication
  - Integration of ERP + CRM
  - Derived columns & normalization
**Gold Layer – Business-ready**
  - Star schema (Fact + Dimensions)
  - Business logic applied
  - Aggregated tables for analytics
  <img width="702" height="434" alt="yuque_diagram 2" src="https://github.com/user-attachments/assets/4cb5008d-1c08-4e83-a699-ddea3c873c87" />
  <img width="546" height="322" alt="yuque_diagram3" src="https://github.com/user-attachments/assets/a76caf1b-2262-461e-8261-5ae1e536fe5a" />
  <img width="564" height="332" alt="yuque_diagram4" src="https://github.com/user-attachments/assets/aebbb507-5d65-40f9-8bdd-e1708f696979" />
  
## 4️⃣ Skills
This project demonstrates proficiency in:
- ETL pipeline development (SQL Server)
- Data cleansing & standardization
- Data Modeling (Star Schema, Medallion Architecture)
- Query optimization

## 5️⃣ Results 
The Gold Layer delivers a unified, analytics-ready data representation designed for business consumption.
It consists of dimension tables and fact tables：
- **dim_customers** – standardized customer profiles with demographic attributes
- **dim_products** – enriched product attributes including categories and pricing
- **fact_sales** – transactional sales facts linked to customer and product dimensions

See the full Data Catalog in **docs/gold_layer_catalog.md**.
  
## 6️⃣ Next Steps & Recommendations
- Visualize data in Power BI for executive dashboards.
- Automate ETL workflows using orchestration tools (Airflow, ADF).
- Enhance analytics with predictive modeling (e.g., forecasting, churn analysis).

