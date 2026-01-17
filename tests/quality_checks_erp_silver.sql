/*
=================================================================================================
Quality Checks
=================================================================================================
Purpose:
These scripts perform various quality checks for data consistency, accuracy, and standardization
across the ERP 'bronze' tables before and after loading data into the 'silver' schema. 
They include checks for:
- Null or duplicate primary keys.
- Unwanted spaces in text fiedls.
- Data normalization/standardization and consistency.
- Invalid date ranges or orders.
- Data consistency between related fields for data integration.

Usage:
- Run these checks before and after loading data into the ERP Silver tables.
- Investigate and resolve any discrepancies found during the checks.
=================================================================================================
*/

/*
=====================================================================================================
Run Quality Checks of Data in bronze.erp_cust_az12 (Column by Column) Before Loading Into Silver
=====================================================================================================
*/

SELECT *
FROM bronze.erp_cust_az12

SELECT *
FROM bronze.crm_cust_info

-- Check if there is any unmatched data between the cid and cst_key columns in the two tables
SELECT
cid AS old_cid,
CASE 
	WHEN cid LIKE 'NAS%' OR LEN(cid) <> 10
		THEN RIGHT(cid, LEN(cid)-3)  -- SUBSTRING(cid, 4, LEN(cid))  
	ELSE cid
END cid,
FROM bronze.erp_cust_az12
WHERE CASE 
	WHEN cid LIKE 'NAS%' OR LEN(cid) <> 10
		THEN RIGHT(cid, LEN(cid)-3)
	ELSE cid
END
NOT IN (
SELECT DISTINCT cst_key 
FROM bronze.crm_cust_info)

-- Identify Out of Range Dates
SELECT DISTINCT 
bdate
FROM bronze.erp_cust_az12
WHERE bdate < '1926-01-01' OR bdate > GETDATE()

-- Check Data Standardization & Consistency
SELECT DISTINCT
gen
FROM bronze.erp_cust_az12

SELECT DISTINCT
gen,
CASE 
	WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
	WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
	ELSE 'n/a'
END gen
FROM bronze.erp_cust_az12

-- Data transformation for all columns in bronze.erp_cust_az12
SELECT
CASE 
	WHEN cid LIKE 'NAS%' OR LEN(cid) <> 10
		THEN RIGHT(cid, LEN(cid)-3)
	ELSE cid
END cid,
CASE 
	WHEN bdate > GETDATE() THEN NULL
    -- WHEN bdate < '1926-01-01' OR bdate > GETDATE() THEN NULL  -- Decided not to delete the very old customers!
	ELSE bdate
END bdate,
CASE 
	WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
	WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
	ELSE 'n/a'
END gen
FROM bronze.erp_cust_az12


/*
===========================================================================================
Re-run Quality Check Queries For the Loaded Data In Silver (silver.erp_cust_az12)
===========================================================================================
*/

-- Identify Out of Range Dates
SELECT DISTINCT 
bdate
FROM silver.erp_cust_az12
WHERE bdate < '1926-01-01' OR bdate > GETDATE()

-- Check Data Standardization & Consistency
SELECT DISTINCT
gen
FROM silver.erp_cust_az12

SELECT *
FROM silver.erp_cust_az12


/*
=====================================================================================================
Run Quality Checks of Data in bronze.erp_loc_a101 (Column by Column) Before Loading Into Silver
=====================================================================================================
*/
  
-- Compare both tables side-by-side
SELECT * 
FROM silver.crm_cust_info

SELECT *
FROM bronze.erp_loc_a101

-- Check the cid column
-- Expectation: No Result
SELECT
cid,
REPLACE(cid, '-', '') AS cid,
cntry
FROM bronze.erp_loc_a101
WHERE REPLACE(cid, '-', '') NOT IN (
SELECT cst_key
FROM silver.crm_cust_info)

-- Check Data Standardization & Consistency for cntry
SELECT DISTINCT
cntry
FROM bronze.erp_loc_a101
ORDER BY 1

-- Compare the original against the transformed
SELECT DISTINCT
cntry AS old_cntry,
CASE 
	WHEN TRIM(cntry) = 'DE' THEN 'Germany'
	WHEN TRIM(cntry) IN ('US', 'USA') THEN 'United States'
	WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'n/a'
	ELSE TRIM(cntry)
END cntry
FROM bronze.erp_loc_a101
ORDER BY 1

-- load all data to silver table silver.erp_loc_a101
INSERT INTO silver.erp_loc_a101(cid, cntry)
SELECT
REPLACE(cid, '-', '') AS cid,
CASE 
	WHEN TRIM(cntry) = 'DE' THEN 'Germany'
	WHEN TRIM(cntry) IN ('US', 'USA') THEN 'United States'
	WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'n/a'
	ELSE TRIM(cntry)
END cntry
FROM bronze.erp_loc_a101

/*
===========================================================================================
Re-run Quality Check Queries For the Loaded Data In Silver (silver.erp_loc_a101)
===========================================================================================
*/

SELECT DISTINCT
cntry
FROM silver.erp_loc_a101
ORDER BY 1

SELECT *
FROM silver.erp_loc_a101

/*
=====================================================================================================
Run Quality Checks of Data in bronze.erp_px_cat_g1v2 (Column by Column) Before Loading Into Silver
=====================================================================================================
*/
  
-- Nothing to be done for the id column as it's already in match with cat_id in the silver.crm_prd_info table
-- Check for Unwanted Spaces for all remaining cols
SELECT *
FROM bronze.erp_px_cat_g1v2
WHERE cat <> TRIM(cat) OR subcat <> TRIM(subcat) OR maintenance <> TRIM(maintenance)

-- Check Data Standardization & Consistency
SELECT DISTINCT
cat
FROM bronze.erp_px_cat_g1v2

SELECT DISTINCT
subcat
FROM bronze.erp_px_cat_g1v2

SELECT DISTINCT
maintenance
FROM bronze.erp_px_cat_g1v2

-- Load all data into the silver.erp_px_cat_g1v2 table
INSERT INTO silver.erp_px_cat_g1v2 (
	id, 
	cat,
	subcat,
	maintenance
)
SELECT
id, 
cat,
subcat,
maintenance
FROM bronze.erp_px_cat_g1v2

/*
===========================================================================================
Re-run Quality Check Queries For the Loaded Data In Silver (silver.erp_px_cat_g1v2)
===========================================================================================
*/

-- Check for Unwanted Spaces
SELECT *
FROM silver.erp_px_cat_g1v2
WHERE cat <> TRIM(cat) OR subcat <> TRIM(subcat) OR maintenance <> TRIM(maintenance)


-- Check Data Standardization & Consistency
SELECT DISTINCT
maintenance
FROM silver.erp_px_cat_g1v2

SELECT *
FROM silver.erp_px_cat_g1v2
