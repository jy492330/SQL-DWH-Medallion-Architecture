/*
=================================================================================================
Quality Checks
=================================================================================================
Purpose:
These scripts perform various quality checks for data consistency, accuracy, and standardization
across the CRM 'bronze' tables before and after loading data into the 'silver' schema. 
They include checks for:
- Null or duplicate primary keys.
- Unwanted spaces in text fiedls.
- Data normalization/standardization and consistency.
- Invalid date ranges or orders.
- Data consistency between related fields for data integration.

Usage:
- Run these checks before and after loading data into the CRM Silver tables.
- Investigate and resolve any discrepancies found during the checks.
=================================================================================================
*/

SELECT TOP 1000 * FROM DWH.bronze.crm_cust_info;
SELECT TOP 1000 * FROM DWH.bronze.crm_prd_info;
SELECT TOP 1000 * FROM DWH.bronze.crm_sales_details;
SELECT TOP 1000 * FROM DWH.bronze.erp_cust_az12;
SELECT TOP 1000 * FROM DWH.bronze.erp_loc_a101;
SELECT TOP 1000 * FROM DWH.bronze.erp_px_cat_g1v2;

/*
===================================================================================================
Run Quality Checks of Data in bronze.crm_cust_info (Column by Column) Before Loading Into Silver
===================================================================================================
*/

SELECT *
FROM bronze.crm_cust_info;

SELECT *
FROM bronze.crm_cust_info
WHERE cst_id = 29466;

-- Check for duplicates and NUlls
SELECT cst_id, COUNT(*)
FROM bronze.crm_cust_info
GROUP BY cst_id
HAVING COUNT(*) > 1 OR cst_id IS NULL;

-- Check for unwanted spaces
-- Expectation: No Results
SELECT cst_firstname
FROM bronze.crm_cust_info
WHERE cst_firstname <> TRIM(cst_firstname)

SELECT cst_lastname
FROM bronze.crm_cust_info
WHERE cst_lastname <> TRIM(cst_lastname)

-- 1st version 
SELECT * 
FROM (
SELECT *, 
ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag
FROM bronze.crm_cust_info
) cte
WHERE flag <> 1;   -- WHERE flag != 1;   WHERE flag = 1;

-- 2nd version
WITH cte AS (
    SELECT *, 
    ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag
    FROM bronze.crm_cust_info
)
SELECT * 
FROM cte
WHERE flag <> 1;  -- WHERE flag != 1;   WHERE flag = 1;


-- Check for unwanted spaces in each column:
-- Expectation: No Results

SELECT cst_firstname
FROM bronze.crm_cust_info
WHERE cst_firstname <> TRIM(cst_firstname) 

SELECT cst_lastname
FROM bronze.crm_cust_info
WHERE cst_lastname <> TRIM(cst_lastname)

SELECT cst_gndr
FROM bronze.crm_cust_info
WHERE cst_gndr <> TRIM(cst_gndr)

-- Transformation script to clean up the columns with unwanted extra spaces

SELECT
cst_id,
cst_key,
TRIM(cst_firstname) AS cst_firstname,
TRIM(cst_lastname) AS cst_lastname,
cst_marital_status,
cst_gndr,
cst_create_date
FROM (
SELECT 
*,
ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag
FROM bronze.crm_cust_info
WHERE cst_id IS NOT NULL
) cte
WHERE flag = 1;   -- WHERE flag = 1 AND cst_id = 29433;


-- Check Data Standardization & Consistency (Get Column Cardinality)
-- 1st Version
SELECT
    cst_id,
    cst_key,
    TRIM(cst_firstname) AS cst_firstname,
    TRIM(cst_lastname) AS cst_lastname,
    CASE 
        WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'  
        WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married' 
        ELSE 'n/a'          -- handling NULLs by replace missing val with default val n/a
    END cst_marital_status, -- Normalize marital status values to readable format
    CASE 
        WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'    -- data enhancement
        WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'      -- data enhancement
        ELSE 'n/a'    -- handling NULLs by replace missing val with default val n/a
    END cst_gndr,     -- Normalize gender values to readable format
    cst_create_date
FROM (
    SELECT 
    *,
    ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag  
    FROM bronze.crm_cust_info
    WHERE cst_id IS NOT NULL  -- filter out all the NULL values first
) cte
WHERE flag = 1;       -- filter out all duplicate rows with older dates by keeping only the most recent record per customer

-- 2nd Version
SELECT
    cst_id,
    cst_key,
    TRIM(cst_firstname) AS cst_firstname,
    TRIM(cst_lastname) AS cst_lastname,
    CASE 
        WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
        WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
        ELSE 'n/a' 
    END cst_marital_status,
    CASE 
        WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
        WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
        ELSE 'n/a'  
    END cst_gndr,
    cst_create_date
FROM (
    SELECT 
    *,
    ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag
    FROM bronze.crm_cust_info
) cte
WHERE flag = 1 AND cst_id IS NOT NULL; 


/*
===================================================
Load Data Into Silver Table (silver.crm_cust_info)
===================================================
*/

INSERT INTO silver.crm_cust_info (
    cst_id,
    cst_key,
    cst_firstname,
    cst_lastname,
    cst_marital_status,
    cst_gndr,
    cst_create_date
)
SELECT
    cst_id,
    cst_key,
    TRIM(cst_firstname) AS cst_firstname,
    TRIM(cst_lastname) AS cst_lastname,
    CASE 
        WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
        WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
        ELSE 'n/a'  
    END cst_marital_status,
    CASE 
        WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
        WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
        ELSE 'n/a' 
    END cst_gndr,
    cst_create_date
FROM (
    SELECT 
    *,
    ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag
    FROM bronze.crm_cust_info
    WHERE cst_id IS NOT NULL
) cte
WHERE flag = 1; 


/*
===========================================================================================
Re-run Quality Check Queries For the Loaded Data In Silver (silver.crm_cust_info)
===========================================================================================
*/

-- Check for duplicates or NUlls in primary id
-- Expectation: No Results
SELECT cst_id, COUNT(*)
FROM silver.crm_cust_info
GROUP BY cst_id
HAVING COUNT(*) > 1 OR cst_id IS NULL;

SELECT cst_key, COUNT(*)
FROM silver.crm_cust_info
GROUP BY cst_key
HAVING COUNT(*) > 1 OR cst_key IS NULL;

-- Check for unwanted spaces for Data Consistency
-- Expectation: No Results
SELECT cst_firstname
FROM silver.crm_cust_info
WHERE cst_firstname <> TRIM(cst_firstname)

SELECT cst_lastname
FROM silver.crm_cust_info
WHERE cst_lastname <> TRIM(cst_lastname)

-- Data Standardization/Normalization + Handing Missing Data By Using Default Value
SELECT DISTINCT cst_marital_status
FROm silver.crm_cust_info

SELECT DISTINCT cst_gndr
FROm silver.crm_cust_info

SELECT * FROM silver.crm_cust_info
ORDER BY cst_id

/*
===================================================================================================
Run Quality Checks of Data in bronze.crm_prd_info (Column by Column) Before Loading Into Silver
Run Quality Checks of Data in bronze.erp_px_cat_g1v2 (Column by Column) Before Loading Into Silver
Run Quality Checks of Data in bronze.crm_sales_details (Column by Column) Before Loading Into Silver
===================================================================================================
*/

SELECT
prd_id,
prd_key,
-- SUBSTRING(prd_key, 1, 5) AS cat_id,
REPLACE(LEFT(prd_key, 5), '-', '_') AS cat_id, 
SUBSTRING(prd_key, 7, LEN(prd_key)) AS prd_key,
prd_nm,
prd_cost,
prd_line,
prd_start_dt,
prd_end_dt
FROM bronze.crm_prd_info
WHERE REPLACE(LEFT(prd_key, 5), '-', '_') NOT IN (  -- check if there's any unmatched category id
SELECT DISTINCT id 
FROM bronze.erp_px_cat_g1v2);

SELECT
prd_id,
prd_key,
-- SUBSTRING(prd_key, 1, 5) AS cat_id,
REPLACE(LEFT(prd_key, 5), '-', '_') AS cat_id, 
SUBSTRING(prd_key, 7, LEN(prd_key)) AS prd_key,
prd_nm,
prd_cost,
prd_line,
prd_start_dt,
prd_end_dt
FROM bronze.crm_prd_info
WHERE SUBSTRING(prd_key, 7, LEN(prd_key)) NOT IN (
SELECT DISTINCT sls_prd_key
FROM bronze.crm_sales_details);

-- Check for unwanted spaces for Data Consistency
-- Expectation: No Results
SELECT prd_nm
FROM bronze.crm_prd_info
WHERE prd_nm <> TRIM(prd_nm)

-- Check for NULLs or Negative Numbers
-- Expectation: No Results
SELECT prd_cost
FROM bronze.crm_prd_info
WHERE prd_cost < 0 OR prd_cost IS NULL

-- Check for Cardinality of prd_line
SELECT DISTINCT prd_line
FROM bronze.crm_prd_info


-- 1st Version (standard format for CASE)
SELECT
prd_id,
prd_key,
-- SUBSTRING(prd_key, 1, 5) AS cat_id,
REPLACE(LEFT(prd_key, 5), '-', '_') AS cat_id, 
SUBSTRING(prd_key, 7, LEN(prd_key)) AS prd_key,
prd_nm,
ISNULL(prd_cost, 0) AS prd_cost,
CASE
	WHEN UPPER(TRIM(prd_line)) = 'M' THEN 'Mountain'
	WHEN UPPER(TRIM(prd_line)) = 'R' THEN 'Road'
	WHEN UPPER(TRIM(prd_line)) = 'S' THEN 'Other Sales'
	WHEN UPPER(TRIM(prd_line)) = 'T' THEN 'Touring'
	ELSE 'n/a'
END AS prd_line,
prd_start_dt,
prd_end_dt
FROM bronze.crm_prd_info

-- 2nd Version (Convenience/Simplified format for CASE - simple value mapping)
SELECT
prd_id,
prd_key,
-- SUBSTRING(prd_key, 1, 5) AS cat_id,
REPLACE(LEFT(prd_key, 5), '-', '_') AS cat_id, 
SUBSTRING(prd_key, 7, LEN(prd_key)) AS prd_key,
prd_nm,
ISNULL(prd_cost, 0) AS prd_cost,
CASE UPPER(TRIM(prd_line))
	WHEN 'M' THEN 'Mountain'
	WHEN 'R' THEN 'Road'
	WHEN 'S' THEN 'Other Sales'
	WHEN 'T' THEN 'Touring'
	ELSE 'n/a'
END AS prd_line,
prd_start_dt,
prd_end_dt
FROM bronze.crm_prd_info

-- Check for Invalid Date Orders
-- End Date Cannot be Earlier Than Start Date
SELECT *
FROM bronze.crm_prd_info
WHERE prd_end_dt < prd_start_dt

-- Replace the original prd_end_dt with the new prd_end_dt definition
SELECT
prd_id,
prd_key,
-- SUBSTRING(prd_key, 1, 5) AS cat_id,
REPLACE(LEFT(prd_key, 5), '-', '_') AS cat_id, 
SUBSTRING(prd_key, 7, LEN(prd_key)) AS prd_key,
prd_nm,
ISNULL(prd_cost, 0) AS prd_cost,
CASE UPPER(TRIM(prd_line))
	WHEN 'M' THEN 'Mountain'
	WHEN 'R' THEN 'Road'
	WHEN 'S' THEN 'Other Sales'
	WHEN 'T' THEN 'Touring'
	ELSE 'n/a'
END AS prd_line,
prd_start_dt,
DATEADD(day, -1, LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt)) AS prd_end_dt
FROM bronze.crm_prd_info

/*
========================================================
Load Data Into Silver Table (silver.crm_prd_info)
========================================================
*/
	
INSERT INTO silver.crm_prd_info (
    prd_id,
    cat_id,
    prd_key,
    prd_nm,
    prd_cost,
    prd_line,
    prd_start_dt,
    prd_end_dt
)
SELECT
prd_id,
REPLACE(LEFT(prd_key, 5), '-', '_') AS cat_id,  -- Alt: REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id
SUBSTRING(prd_key, 7, LEN(prd_key)) AS prd_key,
prd_nm,
ISNULL(prd_cost, 0) AS prd_cost,
CASE UPPER(TRIM(prd_line))
	WHEN 'M' THEN 'Mountain'
	WHEN 'R' THEN 'Road'
	WHEN 'S' THEN 'Other Sales'
	WHEN 'T' THEN 'Touring'
	ELSE 'n/a'
END AS prd_line,
prd_start_dt,
DATEADD(day, -1, LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt)) AS prd_end_dt
FROM bronze.crm_prd_info

/*
===========================================================================================
Re-run Quality Check Queries For the Loaded Data In Silver (silver.crm_prd_info)
===========================================================================================
*/

-- Check for duplicates or NUlls in primary id
-- Expectation: No Results
SELECT prd_id, 
COUNT(*)
FROM silver.crm_prd_info
GROUP BY prd_id
HAVING COUNT(*) > 1 OR prd_id IS NULL

-- Check for unwanted spaces
-- Expectation: No Results
SELECT prd_nm
FROM silver.crm_prd_info
WHERE prd_nm != TRIM(prd_nm)

-- Check for NULLs or Negative Numbers
-- Expectation: No Results
SELECT prd_cost
FROM silver.crm_prd_info
WHERE prd_cost < 0 OR prd_cost IS NULL

-- Data Standardization/Normalization and Consistency 
SELECT DISTINCT prd_line
FROM silver.crm_prd_info

-- Check for Invalid Date Orders
SELECT *
FROM silver.crm_prd_info
WHERE prd_end_dt < prd_start_dt

/*
=====================================================================================================
Run Quality Checks of Data in bronze.crm_sales_details (Column by Column) Before Loading Into Silver
=====================================================================================================
*/

SELECT *
FROM bronze.crm_sales_details
WHERE sls_ord_num != TRIM(sls_ord_num)

-- Option A (Barra's Version)
SELECT *
FROM bronze.crm_sales_details
WHERE sls_prd_key NOT IN (
SELECT prd_key
FROM silver.crm_prd_info) 

SELECT *
FROM bronze.crm_sales_details
WHERE sls_cust_id NOT IN (
SELECT cst_id
FROM silver.crm_cust_info)

-- Option B (ChatGPT Version)
SELECT *
FROM bronze.crm_sales_details s
LEFT JOIN silver.crm_prd_info p
ON s.sls_prd_key = p.prd_key
WHERE p.prd_key IS NULL;

SELECT s.*
FROM bronze.crm_sales_details s
LEFT JOIN silver.crm_cust_info c
ON s.sls_cust_id = c.cst_id
WHERE c.cst_id IS NULL;


-- Check for Invalid Dates & Outliers (sls_order_dt, sls_ship_dt, sls_due_dt)
SELECT 
NULLIF(sls_order_dt, 0) AS sls_order_dt
FROM bronze.crm_sales_details
WHERE sls_order_dt <= 0

SELECT 
NULLIF(sls_order_dt, 0) AS sls_order_dt
FROM bronze.crm_sales_details
WHERE sls_order_dt <= 0
OR LEN(sls_order_dt) != 8
OR sls_order_dt > 20500101
OR sls_order_dt < 20090101

SELECT 
NULLIF(sls_ship_dt, 0) AS sls_ship_dt
FROM bronze.crm_sales_details
WHERE sls_ship_dt <= 0
OR LEN(sls_ship_dt) != 8
OR sls_ship_dt > 20500101
OR sls_ship_dt < 20090101

SELECT 
NULLIF(sls_due_dt, 0) AS sls_due_dt
FROM bronze.crm_sales_details
WHERE sls_due_dt <= 0
OR LEN(sls_due_dt) != 8
OR sls_due_dt > 20500101
OR sls_due_dt < 20090101

-- WIP code for the 3 date columns (sls_order_dt, sls_ship_dt, sls_due_dt) - 1st Version
SELECT 
sls_ord_num,	
sls_prd_key,	
sls_cust_id,	
CASE
	WHEN sls_order_dt = 0 OR LEN(sls_order_dt) != 8 THEN NULL
	ELSE CAST(CAST(sls_order_dt AS CHAR(8)) AS DATE)
END sls_order_dt,
CASE
	WHEN sls_ship_dt = 0 OR LEN(sls_ship_dt) != 8 THEN NULL
	ELSE CAST(CAST(sls_ship_dt AS CHAR(8)) AS DATE)
END sls_ship_dt,
CASE
	WHEN sls_due_dt = 0 OR LEN(sls_due_dt) != 8 THEN NULL
	ELSE CAST(CAST(sls_due_dt AS CHAR(8)) AS DATE)
END sls_due_dt,
sls_sales,	
sls_quantity,	
sls_price
FROM bronze.crm_sales_details

-- WIP code for the 3 date columns (sls_order_dt, sls_ship_dt, sls_due_dt) - 2nd Version
SELECT 
sls_ord_num,	
sls_prd_key,	
sls_cust_id,	
CASE
	WHEN sls_order_dt = 0 OR LEN(CONVERT(VARCHAR(8), sls_order_dt)) <> 8 THEN NULL
	ELSE TRY_CONVERT(date, CONVERT(CHAR(8), sls_order_dt), 112) 
END sls_order_dt,
CASE
	WHEN sls_ship_dt = 0 OR LEN(CONVERT(VARCHAR(8), sls_ship_dt)) <> 8 THEN NULL
	ELSE TRY_CONVERT(date, CONVERT(CHAR(8), sls_ship_dt), 112) 
END sls_ship_dt,
CASE
	WHEN sls_due_dt = 0 OR LEN(CONVERT(VARCHAR(8), sls_due_dt)) <> 8 THEN NULL
	ELSE TRY_CONVERT(date, CONVERT(CHAR(8), sls_due_dt), 112) 
END sls_due_dt,
sls_sales,	
sls_quantity,	
sls_price
FROM bronze.crm_sales_details

-- Check the data quality and consistency of the last three columns
SELECT DISTINCT
sls_ord_num,	
sls_prd_key,	
sls_cust_id,	
CASE
	WHEN sls_order_dt = 0 OR LEN(CONVERT(VARCHAR(8), sls_order_dt)) <> 8 THEN NULL
	ELSE TRY_CONVERT(date, CONVERT(CHAR(8), sls_order_dt), 112) 
END sls_order_dt,
CASE
	WHEN sls_ship_dt = 0 OR LEN(CONVERT(VARCHAR(8), sls_ship_dt)) <> 8 THEN NULL
	ELSE TRY_CONVERT(date, CONVERT(CHAR(8), sls_ship_dt), 112) 
END sls_ship_dt,
CASE
	WHEN sls_due_dt = 0 OR LEN(CONVERT(VARCHAR(8), sls_due_dt)) <> 8 THEN NULL
	ELSE TRY_CONVERT(date, CONVERT(CHAR(8), sls_due_dt), 112) 
END sls_due_dt,
sls_sales,	
sls_quantity,	
sls_price
FROM bronze.crm_sales_details
WHERE sls_sales <> sls_quantity * sls_price   -- check if business rule is applied
OR sls_sales IS NULL OR sls_quantity  IS NULL OR sls_price IS NULL  -- check for NULL 
OR sls_sales <= 0 OR sls_quantity <= 0 OR sls_price <= 0            -- check for zero or negative values
ORDER BY sls_sales, sls_quantity, sls_price;


/*
===========================================================================================
Re-run Quality Check Queries For the Loaded Data In Silver (silver.crm_sales_details)
===========================================================================================
*/

-- Check for Invalid Date Orders
SELECT * 
FROM silver.crm_sales_details
WHERE sls_order_dt > sls_ship_dt OR sls_order_dt > sls_due_dt

-- Check data consistency for sales, quantity and price
-- >> Sales = Quantity * Price
-- >> all 3 value types cannot be zero, negative or NULL.
SELECT DISTINCT
sls_sales,
sls_quantity,	
sls_price
FROM silver.crm_sales_details
WHERE sls_sales <> sls_quantity * sls_price   -- check if business rule is applied
OR sls_sales IS NULL OR sls_quantity  IS NULL OR sls_price IS NULL  -- check for NULL 
OR sls_sales <= 0 OR sls_quantity <= 0 OR sls_price <= 0            -- check for zero or negative values
ORDER BY sls_sales, sls_quantity, sls_price;


