# SQL Data Warehouse with Medallion Architecture (SQL Server)

A modern SQL Server data warehouse project built with a **Medallion architecture** (**Bronze → Silver → Gold**) to consolidate sales data from **ERP + CRM** source systems and deliver **analytics-ready** datasets for reporting and insights.

---

## What this project does

- **Ingests** raw **CSV** extracts from **ERP** and **CRM** source systems
- Loads raw data into a **Bronze** landing zone (traceability + debugging)
- Cleans and standardizes data in a **Silver** layer (ready for analysis)
- Publishes **Gold** reporting objects (views) with business logic and analytics models

---

## High-level Data Architecture (Bronze / Silver / Gold)
![High Level Architecture](https://github.com/jy492330/SQL-DWH-Project/blob/main/docs/diagrams/High%20Level%20Data%20Architecture.png)

### Bronze layer (raw, as-is)
- **Source format:** CSV files (files-in-folder interface)
- **Object type:** Tables 
- **Load method:** Batch processing, **full load (TRUNCATE + INSERT)**  
- **Transformations:** None (raw as-is)

### Silver layer (clean + standardized)
- **Object type:** Tables  
- **Load method:** Batch processing, **full load (TRUNCATE + INSERT)**  
- **Transformations include:**  
  - Data cleansing  
  - Data standardization  
  - Data normalization  
  - Data enrichment  
  - Derived columns 

### Gold layer (business-ready, reporting)

In this project, the **Gold layer is implemented as SQL views** (no physical load step). It is where the warehouse becomes **business-ready** for BI/reporting and ad-hoc analysis. 

**Gold layer characteristics**
- **Object type:** Views (semantic/reporting layer) 
- **Load method:** None (views)
- **Transformations:** data integration, aggregations, business logic 

#### Gold data models (3 supported patterns)

Your Gold layer is designed to support **three different data modeling outputs**: 

#### Option A — Star schema 
**Data Flow Diagram (Option A: Star Schema)**
![Data Flow Diagram - Star Schema](https://github.com/jy492330/SQL-DWH-Project/blob/main/docs/diagrams/Data%20Flow%20Diagram%20(Star%20Schema).png)

**Gold objects**
- `dim_customers`
- `dim_products`
- `fact_sales` 

This pattern keeps dimensions separate and connects them to a central fact table (standard star schema build). 

#### Option B — Flat / Wide table (denormalized)
**Data Flow Diagram (Option B: Flat Table)**

This option produces **one wide dataset** where customer + product attributes are already joined onto each sales row.

Typical Gold object examples (names are just examples)
- `gold.vw_sales_wide` (view)
- or `gold.sales_wide` (table, if you ever choose to materialize it)

**Conceptual build**
- Join `silver.crm_sales_details`
  with `silver.crm_cust_info` (+ ERP customer/location tables as needed)
  and `silver.crm_prd_info` (+ ERP category tables as needed)
- Output **one wide Gold object** for BI tools and ad-hoc analysis

#### Option C — Aggregated tables (summary marts)
This option produces **pre-aggregated reporting objects** for faster dashboards (for example: by day/week/month, by product, by customer segment).

Typical Gold object examples (names are just examples)
- `gold.vw_sales_daily`
- `gold.vw_sales_monthly_by_product`
- `gold.vw_customer_summary`

These objects apply grouping/aggregation logic in Gold (still typically as views).

---

## Data sources

This project integrates two source systems:
- **CRM** (CSV extracts)
- **ERP** (CSV extracts)
  
![Data Integration Diagram](https://github.com/jy492330/SQL-DWH-Project/blob/main/docs/diagrams/Integration%20Model.png)
---

## Data flow (tables by layer)

From the data flow diagram, the pipeline follows this structure: 

### Bronze tables
- CRM: `crm_sales_details`, `crm_cust_info`, `crm_prd_info`
- ERP: `erp_cust_az12`, `erp_loc_a101`, `erp_px_cat_g1v2` 

### Silver tables
- `crm_sales_details`, `crm_cust_info`, `crm_prd_info`
- `erp_cust_az12`, `erp_loc_a101`, `erp_px_cat_g1v2`

### Gold model (star schema)
- Dimensions: `dim_customers`, `dim_products`
- Fact: `fact_sales` 

---

## Analytics goals 

The Gold layer is designed to support SQL-based analytics and reporting, including:
- Customer behavior insights
- Product performance analysis
- Sales trend reporting 

---

## Repository structure

Typical layout:
- `datasets/` – source CSV files (ERP + CRM extracts)
- `scripts/` – DDL + stored procedures for Bronze/Silver/Gold
- `docs/` – diagrams and documentation
- `tests/` – validation scripts (optional)

---

## How to run (local SQL Server)

### Prerequisites
- SQL Server (local or dev instance)
- SQL Server Management Studio (SSMS)
- Source CSV files for ERP + CRM

### Steps
1. **Clone** the repository.
2. Place your **ERP/CRM CSV files** into the folder location expected by your load scripts.
3. Run the **DDL scripts** to create schemas and tables for:
   - Bronze tables
   - Silver tables
   - Gold views
4. Execute the **load stored procedures** for Bronze and Silver (names may vary in your repo). Example:
   - `EXEC bronze.load_bronze;`
   - `EXEC silver.load_silver;`
5. Query the **Gold views** (star schema) for reporting and analytics.

---

## Notes on design choices

- Bronze and Silver use **full refresh** loads (**TRUNCATE + INSERT**) to keep the warehouse aligned with the latest source extracts.   
- Gold uses **views** (no physical load) to keep reporting logic centralized and easy to change. 

---

## License

MIT (see `LICENSE`).

