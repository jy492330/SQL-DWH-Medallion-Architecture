/*
=========================================================================================
DDL Script: Create Gold Views
=========================================================================================
This script creates view objects for the Gold layer in the Data Warehouse.
The Gold layer represents the final dimension and fact views (Star Schema)

Each view performs transformations and integrates data from the Silver layer
to produce a clean, enriched and business-ready dataset.

Usage: 
These views can be queried directly for analytics and BI reporting.
=========================================================================================
*/


-- ======================================================================================
-- Create Dimension: gold.dim_customers
-- ======================================================================================
CREATE OR ALTER VIEW gold.dim_customers AS
SELECT
	ROW_NUMBER() OVER (ORDER BY c1.cst_id) AS customer_key, 
	c1.cst_id AS customer_id,
	c1.cst_key AS customer_number,
	c1.cst_firstname AS first_name,
	c1.cst_lastname AS last_name,
	c3.cntry AS country,
	c1.cst_marital_status AS marital_status,
	CASE 
		WHEN c1.cst_gndr <> 'n/a' THEN c1.cst_gndr
		ELSE COALESCE(c2.gen, 'n/a')
	END AS gender,
	c2.bdate AS birthdate,
	c1.cst_create_date AS create_date
FROM silver.crm_cust_info c1
LEFT JOIN silver.erp_cust_az12 c2
ON c1.cst_key = c2.cid
LEFT JOIN silver.erp_loc_a101 c3
ON c1.cst_key = c3.cid

  
-- ======================================================================================
-- Create Dimension: gold.dim_products
-- ======================================================================================
CREATE OR ALTER VIEW gold.dim_products AS
SELECT
	ROW_NUMBER() OVER (ORDER BY p1.prd_start_dt, p1.prd_key) AS product_key,
	p1.prd_id AS product_id,
	p1.prd_key AS product_number,
	p1.prd_nm AS product_name,
	p1.cat_id AS category_id,
	p2.cat AS category,
	p2.subcat AS subcategory,
	p2.maintenance,
	p1.prd_cost AS cost,
	p1.prd_line AS product_line,
	p1.prd_start_dt AS start_date
FROM silver.crm_prd_info p1
LEFT JOIN silver.erp_px_cat_g1v2 p2
ON p1.cat_id = p2.id
WHERE p1.prd_end_dt IS NULL 

  
-- ======================================================================================
-- Create Fact: gold.fact_sales
-- ======================================================================================
CREATE OR ALTER VIEW gold.fact_sales AS
SELECT 
sd.sls_ord_num AS order_number,
pr.product_key,   -- Surrogate Key from Gold Dim Products View
cu.customer_key,  -- Surrogate Key from Gold Dim Customers View
sd.sls_order_dt AS order_date,
sd.sls_ship_dt AS shipping_date,
sd.sls_due_dt AS due_date,
sd.sls_sales AS sales_amount,
sd.sls_quantity AS quantity,
sd.sls_price AS price
FROM silver.crm_sales_details sd    
LEFT JOIN gold.dim_products pr      
ON sd.sls_prd_key = pr.product_number
LEFT JOIN gold.dim_customers cu    
ON sd.sls_cust_id = cu.customer_id

