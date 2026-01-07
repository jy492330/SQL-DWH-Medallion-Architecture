/*
==========================================================================
 Stored Procedure: Load Bronze Layer (Source -> Landing Zone)
==========================================================================
This stored procedure loads data into the 'bronze' schema (landing zone)
from external CSV files. It performs the following load actions to ensure 
data-idempotency(full refresh):

- Truncates the tables before loading data.
- Uses 'BULK INSERT' command to load data from CSV files to the bronze tables.

Usage Example (This command should be implemented in a separate query in SSMS):
    EXEC bronze.load_bronze; 
==========================================================================
*/

CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN
	SET NOCOUNT ON; 
	DECLARE @batch_start_time DATETIME,
			@batch_end_time   DATETIME,
			@start_time       DATETIME,
			@end_time         DATETIME;

	BEGIN TRY
		SET @batch_start_time = GETDATE();
		PRINT '======================================================='
		PRINT 'Loading Bronze Layer';
		PRINT '======================================================='
		PRINT '';
		PRINT '-------------------------------------------------------'
		PRINT 'Loading CRM Tables';
		PRINT '-------------------------------------------------------'
	
		PRINT '';
		SET @start_time = GETDATE();
		PRINT '>> Truncating + Inserting Data Into Table: bronze.crm_cust_info';
		TRUNCATE TABLE bronze.crm_cust_info;  
		BULK INSERT bronze.crm_cust_info
		FROM 'C:\WORKSPACE\Data Engineering\Projects\Data Warehouse SQL Project (Jessica)\datasets\source_crm\cust_info.csv'
		WITH (
		FIRSTROW = 2,
		FIELDTERMINATOR = ',',
		TABLOCK  
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR(50)) + ' seconds';
		PRINT '>> ------------------------';

		PRINT '';
		SET @start_time = GETDATE();
		PRINT '>> Truncating + Inserting Data Into Table: bronze.crm_prd_info';
		TRUNCATE TABLE bronze.crm_prd_info; 
		BULK INSERT bronze.crm_prd_info
		FROM 'C:\WORKSPACE\Data Engineering\Projects\Data Warehouse SQL Project (Jessica)\datasets\source_crm\prd_info.csv'
		WITH (
		FIRSTROW = 2,
		FIELDTERMINATOR = ',',
		TABLOCK   
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR(50)) + ' seconds';
		PRINT '>> ------------------------';

		PRINT '';
		SET @start_time = GETDATE();
		PRINT '>> Truncating + Inserting Data Into Table: bronze.crm_sales_details';
		TRUNCATE TABLE bronze.crm_sales_details; 
		BULK INSERT bronze.crm_sales_details
		FROM 'C:\WORKSPACE\Data Engineering\Projects\Data Warehouse SQL Project (Jessica)\datasets\source_crm\sales_details.csv'
		WITH (
		FIRSTROW = 2,
		FIELDTERMINATOR = ',',
		TABLOCK   
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR(50)) + ' seconds';
		PRINT '>> ------------------------';

		PRINT '';
		PRINT '-------------------------------------------------------';
		PRINT 'Loading ERP Tables';
		PRINT '-------------------------------------------------------';

		PRINT '';
		SET @start_time = GETDATE();
		PRINT '>> Truncating + Inserting Data Into Table: bronze.erp_cust_az12';
		TRUNCATE TABLE bronze.erp_cust_az12; 
		BULK INSERT bronze.erp_cust_az12
		FROM 'C:\WORKSPACE\Data Engineering\Projects\Data Warehouse SQL Project (Jessica)\datasets\source_erp\CUST_AZ12.csv'
		WITH (
		FIRSTROW = 2,
		FIELDTERMINATOR = ',',
		TABLOCK   
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR(50)) + ' seconds';
		PRINT '>> ------------------------';

		PRINT '';
		SET @start_time = GETDATE();
		PRINT '>> Truncating + Inserting Data Into Table: bronze.erp_loc_a101';
		TRUNCATE TABLE bronze.erp_loc_a101; 
		BULK INSERT bronze.erp_loc_a101
		FROM 'C:\WORKSPACE\Data Engineering\Projects\Data Warehouse SQL Project (Jessica)\datasets\source_erp\LOC_A101.csv'
		WITH (
		FIRSTROW = 2,
		FIELDTERMINATOR = ',',
		TABLOCK   
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR(50)) + ' seconds';
		PRINT '>> ------------------------';

		PRINT '';
		SET @start_time = GETDATE();
		PRINT '>> Truncating + Inserting Data Into Table: bronze.erp_px_cat_g1v2';
		TRUNCATE TABLE bronze.erp_px_cat_g1v2; 
		BULK INSERT bronze.erp_px_cat_g1v2
		FROM 'C:\WORKSPACE\Data Engineering\Projects\Data Warehouse SQL Project (Jessica)\datasets\source_erp\PX_CAT_G1V2.csv'
		WITH (
		FIRSTROW = 2,
		FIELDTERMINATOR = ',',
		TABLOCK   
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR(50)) + ' seconds';
		PRINT '>> ------------------------';

		SET @batch_end_time = GETDATE();
		PRINT '';
		PRINT '======================================================='
		PRINT 'Loading Bronze Layer Is Completed';
		PRINT '- Total Load Duration: ' + CAST(DATEDIFF(second, @batch_start_time, @batch_end_time) AS NVARCHAR(50)) + ' seconds';
		PRINT '======================================================='
	END TRY

	BEGIN CATCH
		DECLARE 
		@error_number   INT            =  ERROR_NUMBER(),
		@error_line     INT            =  ERROR_LINE(),
		@error_state    INT            =  ERROR_STATE(),
		@error_severity INT            =  ERROR_SEVERITY(),
		@error_proc     NVARCHAR(128)  =  ISNULL(ERROR_PROCEDURE(), N'(ad-hoc)'),
		@error_message  NVARCHAR(4000) =  ERROR_MESSAGE();

		PRINT '=======================================================';
		PRINT 'ERROR OCCURRED DURING LOADING BRONZE LAYER';
		PRINT 'Procedure: ' + @error_proc;
		PRINT 'Line: '      + CAST(@error_line AS NVARCHAR(20));
		PRINT 'Number: '    + CAST(@error_number AS NVARCHAR(20));
		PRINT 'Severity: '  + CAST(@error_severity AS NVARCHAR(20));
		PRINT 'State: '     + CAST(@error_state AS NVARCHAR(20));
		PRINT 'Message: '   + @error_message;
		PRINT '=======================================================';
		
		THROW; 
	END CATCH
END
