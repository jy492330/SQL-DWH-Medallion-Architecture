/*
============================================================================
Quality Checks 
============================================================================
This script performs quality checks to validate the integrity, consistency,
and accuracy of the GOld layer. These checks ensure:
* Uniqueness of surrogate keys in dimension tables.
* Referential integrity between fact and dimension tables.
* Validation of relationships in the data model for analytics purposes.

Usage Notes:
* Run these checks after data loading Silver layer.
* Investigate and resolve any discrepancies found during the checks.
============================================================================
*/

-------------------------------------------------------- Checking 'gold.dim_customers' --------------------------------------------  
-- Check if there’s any duplicate by prd_key after combining the customers tables
SELECT cst_id, COUNT(*)
FROM (
SELECT
	c1.cst_id,
	c1.cst_key,
	c1.cst_firstname,
	c1.cst_lastname,
	c1.cst_marital_status,
	CASE 
		WHEN c1.cst_gndr <> 'n/a' THEN c1.cst_gndr
		ELSE COALESCE(c2.gen, 'n/a')
	END new_gen,              -- Data Integration
	c1.cst_create_date,
	c2.bdate,
	c3.cntry
FROM silver.crm_cust_info c1
LEFT JOIN silver.erp_cust_az12 c2
ON c1.cst_key = c2.cid
LEFT JOIN silver.erp_loc_a101 c3
ON c1.cst_key = c3.cid
) cte
GROUP BY cst_id
HAVING COUNT(*) > 1

-------------------------------------------------------- Checking 'gold.dim_products' --------------------------------------------  
-- Check if there’s any duplicate by prd_key after combining the products tables
SELECT prd_key, COUNT(*)
FROM (
SELECT
	p1.prd_id,
	p1.cat_id,
	p1.prd_key,
	p1.prd_nm,
	p1.prd_cost,
	p1.prd_line,
	p1.prd_start_dt,
	p2.cat,
	p2.subcat,
	p2.maintenance
FROM silver.crm_prd_info p1
LEFT JOIN silver.erp_px_cat_g1v2 p2
ON p1.cat_id = p2.id
WHERE prd_end_dt IS NULL  -- filter out all historical data
) cte
GROUP BY prd_key
HAVING COUNT(*) > 1

-- Check the Data Quality in the View Object
SELECT *
FROM gold.dim_products

-------------------------------------------------------- Checking 'gold.fact_sales' --------------------------------------------
  
SELECT *
FROM silver.crm_sales_details

SELECT *
FROM gold.dim_customers

SELECT *
FROM gold.fact_sales
ORDER BY order_number

SELECT *
FROM gold.fact_sales f
LEFT JOIN gold.dim_customers c
ON f.customer_key = c.customer_key
LEFT JOIN gold.dim_products p
ON f.product_key = p.product_key
WHERE p.product_key IS NULL
