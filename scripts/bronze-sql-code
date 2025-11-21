/*
=====================================================================================================
Stored Procedure: Load Bronze Layer
=====================================================================================================
Script Purpose:
	This stored procedure loads data into the 'bronze' schema from external csv files.
	It performs the following actions:
	- Truncates the bronze tables before loading data.
	- Uses the 'Bulk Insert' command to load data from csv files to bronze tables.
Usage Example:
	EXEC bronze.load_bronze;
=====================================================================================================
*/

-- Check if alreadt existing a database called 'DataWarehouse',if so drop it and recreate a new one.


-- Create Database 'DataWarehouse'

USE master;

CREATE DATABASE DataWarehouse;

USE DataWarehouse;

-- Create 3 Schemas
GO
CREATE SCHEMA bronze;
GO
CREATE SCHEMA silver;
GO
CREATE SCHEMA gold;
GO

-- Create tables for the bronze layer
IF OBJECT_ID('bronze.crm_cust_info','U') IS NOT NULL
	DROP TABLE bronze.crm_cust_info;
CREATE TABLE bronze.crm_cust_info(
	cst_id INT,
	cst_key VARCHAR(50),
	cst_firstname VARCHAR(50),
	cst_lastname VARCHAR(50),
	cst_marital_status VARCHAR(50),
	cst_gndr VARCHAR(50),
	cst_create_date DATE
);

IF OBJECT_ID('bronze.crm_prd_info','U') IS NOT NULL
	DROP TABLE bronze.crm_prd_info;
CREATE TABLE bronze.crm_prd_info(
	prd_id INT,
	prd_key VARCHAR(50),
	prd_nm VARCHAR(50),
	prd_cost INT,
	prd_line VARCHAR(50),
	prd_start_dt DATETIME,
	prd_end_dt DATETIME
);

IF OBJECT_ID('bronze.crm_sales_details','U') IS NOT NULL
	DROP TABLE bronze.crm_sales_details;
CREATE TABLE bronze.crm_sales_details(
	sls_ord_num VARCHAR(50),
	sls_prd_key VARCHAR(50),
	sls_cust_id INT,
	sls_order_dt INT,
	sls_ship_dt INT,
	sls_due_dt INT,
	sls_sales INT,
	sls_quantity INT,
	sls_price INT
);

IF OBJECT_ID('bronze.erp_CUST_AZ12','U') IS NOT NULL
	DROP TABLE bronze.erp_CUST_AZ12;
CREATE TABLE bronze.erp_CUST_AZ12(
	CID VARCHAR(50),
	BDATE DATE,
	GEN VARCHAR(50)
);	

IF OBJECT_ID('bronze.erp_LOC_A101','U') IS NOT NULL
	DROP TABLE bronze.erp_LOC_A101;
CREATE TABLE bronze.erp_LOC_A101(
	CID VARCHAR(50),
	CNTRY VARCHAR(50)
);	

IF OBJECT_ID('bronze.erp_PX_CAT_G1V2','U') IS NOT NULL
	DROP TABLE bronze.erp_PX_CAT_G1V2;
CREATE TABLE bronze.erp_PX_CAT_G1V2(
	ID VARCHAR(50),
	CAT VARCHAR(50),
	SUBCAT VARCHAR(50),
	MAINTENANCE VARCHAR(50)
);	

-- Develop SQL Load Scripts and Create Stored Procedure	
EXEC bronze.load_bronze

GO
CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN
	DECLARE @start_time DATETIME,@end_time DATETIME,@batch_start_time DATETIME,@batch_end_time DATETIME
	BEGIN TRY
		SET @batch_start_time = GETDATE();
		PRINT'=====================================================';
		PRINT'Loading Bronze Layer';
		PRINT'=====================================================';

		PRINT'-----------------------------------------------------';
		PRINT'Loading CRM Tables';
		PRINT'-----------------------------------------------------';

		SET @start_time = GETDATE();
		PRINT'**Truncate and Insert Table:bronze.crm_cust_info**';
		TRUNCATE TABLE bronze.crm_cust_info;
		BULK INSERT bronze.crm_cust_info
		FROM 'C:\Users\z0053z7r\OneDrive - Siemens AG\Desktop\infor-basic\SQL Datawarehouse Project\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();	
		PRINT'** Load Duration: '+ CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR(50))+' seconds';
		PRINT'-----------------------------------------------------';

		SET @start_time = GETDATE();
		PRINT'**Truncate and Insert Table:bronze.crm_prd_info**';
		TRUNCATE TABLE bronze.crm_prd_info;
		BULK INSERT bronze.crm_prd_info
		FROM 'C:\Users\z0053z7r\OneDrive - Siemens AG\Desktop\infor-basic\SQL Datawarehouse Project\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();	
		PRINT'** Load Duration: '+ CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR(50))+' seconds';
		PRINT'-----------------------------------------------------';

		SET @start_time = GETDATE();
		PRINT'**Truncate and Insert Table:bronze.crm_sales_details**';
		TRUNCATE TABLE bronze.crm_sales_details;
		BULK INSERT bronze.crm_sales_details
		FROM 'C:\Users\z0053z7r\OneDrive - Siemens AG\Desktop\infor-basic\SQL Datawarehouse Project\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();	
		PRINT'** Load Duration: '+ CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR(50))+' seconds';
		PRINT'-----------------------------------------------------';

		PRINT'-----------------------------------------------------';
		PRINT'Loading ERP Tables';
		PRINT'-----------------------------------------------------';

		SET @start_time = GETDATE();
		PRINT'**Truncate and Insert Table:bronze.erp_cust_az12**';
		TRUNCATE TABLE bronze.erp_cust_az12;
		BULK INSERT bronze.erp_cust_az12
		FROM 'C:\Users\z0053z7r\OneDrive - Siemens AG\Desktop\infor-basic\SQL Datawarehouse Project\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();	
		PRINT'** Load Duration: '+ CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR(50))+' seconds';
		PRINT'-----------------------------------------------------';

		SET @start_time = GETDATE();
		PRINT'**Truncate and Insert Table:bronze.erp_loc_a101**';
		TRUNCATE TABLE bronze.erp_loc_a101;
		BULK INSERT bronze.erp_loc_a101
		FROM 'C:\Users\z0053z7r\OneDrive - Siemens AG\Desktop\infor-basic\SQL Datawarehouse Project\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();	
		PRINT'** Load Duration: '+ CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR(50))+' seconds';
		PRINT'-----------------------------------------------------';

		SET @start_time = GETDATE();
		PRINT'**Truncate and Insert Table:bronze.erp_px_cat_g1v2**';
		TRUNCATE TABLE bronze.erp_px_cat_g1v2;
		BULK INSERT bronze.erp_px_cat_g1v2
		FROM 'C:\Users\z0053z7r\OneDrive - Siemens AG\Desktop\infor-basic\SQL Datawarehouse Project\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();	
		PRINT'** Load Duration: '+ CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR(50))+' seconds';
		PRINT'-----------------------------------------------------';

		SET @batch_end_time = GETDATE();
		PRINT'Loading Bronze Layer is completed.';
		PRINT'** Total Load Duration： '+ CAST(DATEDIFF(second,@batch_start_time,@batch_end_time) AS NVARCHAR) + ' seconds';
	END TRY
	BEGIN CATCH
		PRINT'=====================================================';
		PRINT'ERROR OCCURED DURING LOADING BRONZE LAYER';
		PRINT'ERROR MESSAGE'+ CAST(ERROR_NUMBER() AS NVARCHAR);
		PRINT'=====================================================';
	END CATCH
END
