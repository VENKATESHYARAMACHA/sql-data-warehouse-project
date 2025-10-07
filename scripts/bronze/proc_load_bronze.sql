/*
===============================================================================================
Stored Procedure: Loading Bronze Layer (Source -> Bronze)
===============================================================================================
Script Purpose:
	This Stored Procedure loads the data into the 'bronze' schema from external CSV files.
	It performs the following actions:
	- Truncates the bronze tables before loading the data.
	- Uses the 'BULK INSERT' command to load the csv files data into bronze tables.
parameters:
	None.
	This stored procedure doesnot accpet any parameters or return any.
Usage Example: 
	Exec proc bronze.load_bronze;
===============================================================================================
*/
CREATE OR ALTER PROC bronze.load_bronze AS
BEGIN
	DECLARE @start_time DATETIME,@end_time DATETIME,
			@batch_start_time DATETIME,@batch_end_time DATETIME;
	BEGIN TRY
		SET @batch_start_time=GETDATE();
		PRINT '=======================================';
		PRINT 'Loading Bronze Layer';
		PRINT '=======================================';
		PRINT '---------------------------------------';
		PRINT 'Loading CRM Tables';
		PRINT '---------------------------------------';
		SET @start_time=GETDATE();
		PRINT '>> Truncating Table: bronze.crm_cust_info';
		TRUNCATE TABLE bronze.crm_cust_info;
		PRINT '>> Loading Data into Table: bronze.crm_cust_info';
		BULK INSERT bronze.crm_cust_info
		FROM 'E:\Venkatesh\1. SQL\Baraa\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
		WITH
		(
			FIRSTROW=2,
			FIELDTERMINATOR=',',
			TABLOCK
		);
		SET @end_time=GETDATE();
		PRINT 'LOAD DURATION: '+ CAST(DATEDIFF(SECOND,@start_time,@end_time) AS VARCHAR) + ' SECONDS';
		PRINT '>>------------------------------------'
		
		SET @start_time=GETDATE();
		PRINT '>> Truncating Table: bronze.crm_prd_info';
		TRUNCATE TABLE bronze.crm_prd_info;
		PRINT '>> Loading Data into Table: bronze.crm_prd_info';
		BULK INSERT bronze.crm_prd_info
		FROM 'E:\Venkatesh\1. SQL\Baraa\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
		WITH
		(
			FIRSTROW=2,
			FIELDTERMINATOR=',',
			TABLOCK
		);
		SET @end_time=GETDATE();
		PRINT 'LOAD DURATION: '+ CAST(DATEDIFF(SECOND,@start_time,@end_time) AS VARCHAR) + ' SECONDS';
		PRINT '>>------------------------------------'


		SET @start_time=GETDATE();
		PRINT '>> Truncating Table: bronze.crm_sales_details';
		TRUNCATE TABLE bronze.crm_sales_details;
		PRINT '>> Loading Data into Table: bronze.crm_sales_details';
		BULK INSERT bronze.crm_sales_details
		FROM 'E:\Venkatesh\1. SQL\Baraa\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
		WITH
		(
			FIRSTROW=2,
			FIELDTERMINATOR=',',
			TABLOCK
		);
		SET @end_time=GETDATE();
		PRINT 'LOAD DURATION: '+ CAST(DATEDIFF(SECOND,@start_time,@end_time) AS VARCHAR) + ' SECONDS';
		PRINT '>>------------------------------------'


		PRINT '---------------------------------------';
		PRINT 'Loading CRM Tables';
		PRINT '---------------------------------------'
		
		SET @start_time=GETDATE();
		PRINT '>> Truncating Table: bronze.erp_CUST_AZ12';
		TRUNCATE TABLE bronze.erp_CUST_AZ12;
		PRINT '>> Loading Data into Table: bronze.erp_CUST_AZ12';
		BULK INSERT bronze.erp_CUST_AZ12
		FROM 'E:\Venkatesh\1. SQL\Baraa\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
		WITH
		(
			FIRSTROW=2,
			FIELDTERMINATOR=',',
			TABLOCK
		);
		SET @end_time=GETDATE();
		PRINT 'LOAD DURATION: '+ CAST(DATEDIFF(SECOND,@start_time,@end_time) AS VARCHAR) + ' SECONDS';
		PRINT '>>------------------------------------'


		SET @start_time=GETDATE();
		PRINT '>> Truncating Table: bronze.erp_LOC_A101';
		TRUNCATE TABLE bronze.erp_LOC_A101;
		PRINT '>> Loading Data into Table: bronze.erp_LOC_A101';
		BULK INSERT bronze.erp_LOC_A101
		FROM 'E:\Venkatesh\1. SQL\Baraa\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
		WITH
		(
			FIRSTROW=2,
			FIELDTERMINATOR=',',
			TABLOCK
		);
		SET @end_time=GETDATE();
		PRINT 'LOAD DURATION: '+ CAST(DATEDIFF(SECOND,@start_time,@end_time) AS VARCHAR) + ' SECONDS';
		PRINT '>>------------------------------------'
		

		SET @start_time=GETDATE();
		PRINT '>> Truncating Table: bronze.erp_PX_CAT_G1V2';
		TRUNCATE TABLE bronze.erp_PX_CAT_G1V2;
		PRINT '>> Loading Data into Table: bronze.erp_PX_CAT_G1V2';
		BULK INSERT bronze.erp_PX_CAT_G1V2
		FROM 'E:\Venkatesh\1. SQL\Baraa\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
		WITH
		(
			FIRSTROW=2,
			FIELDTERMINATOR=',',
			TABLOCK
		);
		SET @end_time=GETDATE();
		PRINT 'LOAD DURATION: '+ CAST(DATEDIFF(SECOND,@start_time,@end_time) AS VARCHAR) + ' SECONDS';
		PRINT '>>------------------------------------'

		SET @batch_end_time=GETDATE();
		PRINT '======================================';
		PRINT 'LOADING BRONZE LAYER IS COMPLETED';
		PRINT 'TOTAL LOAD DURATION: '+ CAST(DATEDIFF(SECOND,@batch_start_time,@batch_end_time) AS VARCHAR) + ' SECONDS';
		PRINT '======================================';

	END TRY
	BEGIN CATCH
		PRINT '================================================';
		PRINT 'ERROR OCCURED DURING BRONZE LAYER LOADING:';
		PRINT 'ERROR MESSAGE:'+ ERROR_MESSAGE();
		PRINT 'ERROR CODE:'+ CAST(ERROR_NUMBER() AS NVARCHAR);
		PRINT 'ERROR STATE:'+ CAST(ERROR_STATE() AS NVARCHAR);
		PRINT '================================================';

	END CATCH

END
