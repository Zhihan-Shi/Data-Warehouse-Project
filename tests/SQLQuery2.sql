-- Create tables for the silver layer
IF OBJECT_ID('silver.crm_cust_info','U') IS NOT NULL
	DROP TABLE silver.crm_cust_info;
CREATE TABLE silver.crm_cust_info(
	cst_id INT,
	cst_key VARCHAR(50),
	cst_firstname VARCHAR(50),
	cst_lastname VARCHAR(50),
	cst_marital_status VARCHAR(50),
	cst_gndr VARCHAR(50),
	cst_create_date DATE,
	dwh_create_date	DATETIME2 DEFAULT GETDATE()
); 

IF OBJECT_ID('silver.crm_prd_info','U') IS NOT NULL
	DROP TABLE silver.crm_prd_info;
CREATE TABLE silver.crm_prd_info(
	prd_id INT,
	cat_id VARCHAR(50),
	prd_key VARCHAR(50),
	prd_nm VARCHAR(50),
	prd_cost INT,
	prd_line VARCHAR(50),
	prd_start_dt DATE,
	prd_end_dt DATE,
	dwh_create_date	DATETIME2 DEFAULT GETDATE()
);

IF OBJECT_ID('silver.crm_sales_details','U') IS NOT NULL
	DROP TABLE silver.crm_sales_details;
CREATE TABLE silver.crm_sales_details(
	sls_ord_num VARCHAR(50),
	sls_prd_key VARCHAR(50),
	sls_cust_id INT,
	sls_order_dt DATE,
	sls_ship_dt DATE,
	sls_due_dt DATE,
	sls_sales INT,
	sls_quantity INT,
	sls_price INT,
	dwh_create_date	DATETIME2 DEFAULT GETDATE()
);

IF OBJECT_ID('silver.erp_cust_az12','U') IS NOT NULL
	DROP TABLE silver.erp_cust_az12;
CREATE TABLE silver.erp_cust_az12(
	CID VARCHAR(50),
	BDATE DATE,
	GEN VARCHAR(50),
	dwh_create_date	DATETIME2 DEFAULT GETDATE()
);	

IF OBJECT_ID('silver.erp_loc_a101','U') IS NOT NULL
	DROP TABLE silver.erp_loc_a101;
CREATE TABLE silver.erp_loc_a101(
	CID VARCHAR(50),
	CNTRY VARCHAR(50),
	dwh_create_date	DATETIME2 DEFAULT GETDATE()
);	

IF OBJECT_ID('silver.erp_px_cat_g1v2','U') IS NOT NULL
	DROP TABLE silver.erp_px_cat_g1v2;
CREATE TABLE silver.erp_px_cat_g1v2(
	ID VARCHAR(50),
	CAT VARCHAR(50),
	SUBCAT VARCHAR(50),
	MAINTENANCE VARCHAR(50),
	dwh_create_date	DATETIME2 DEFAULT GETDATE()
);	

/* Clean & Load crm_cust_info. */
-- 1. Check for Nulls or Duplicates in Primary Key
-- Expectation： No Result
SELECT 
cst_id,
COUNT(*)
FROM bronze.crm_cust_info
GROUP BY cst_id
HAVING COUNT(*)>1;

-- Check an example for details
SELECT *
FROM bronze.crm_cust_info
WHERE cst_id = 29466;

-- Remove duplicates and Keep only the latest record.
SELECT *
FROM
	(SELECT 
	*,
	ROW_NUMBER() OVER(PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last
	FROM bronze.crm_cust_info)t
WHERE flag_last = 1;

-- 2. Check for unwanted Spaces，Data Standardization & Consistency.
SELECT cst_gndr
FROM bronze.crm_cust_info
WHERE cst_gndr != TRIM(cst_gndr);

-- 3. Clean & Load into the table
PRINT '>> Truncating Table:silver.crm_cust_info';
TRUNCATE TABLE silver.crm_cust_info
PRINT '>> Inserting Data Into:silver.crm_cust_info';
INSERT INTO silver.crm_cust_info (
	cst_id,
    cst_key,
	cst_firstname,
	cst_lastname,
	cst_marital_status,
	cst_gndr,
	cst_create_date)
SELECT 
cst_id,
cst_key,
TRIM(cst_firstname) AS cst_firstname,
TRIM(cst_lastname) AS cst_lastname,
CASE WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
     WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
	 ELSE 'n/a'
END cst_marital_status, -- Normalize marital status values to readable formate.
CASE WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
     WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
	 ELSE 'n/a'
END cst_gndr, -- Normalize gender status values to readable formate.
cst_create_date
FROM (
		SELECT *
		FROM
			(SELECT 
			*,
			ROW_NUMBER() OVER(PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last
			FROM bronze.crm_cust_info)t
		WHERE flag_last = 1 AND cst_id IS NOT NULL -- select the most relevant record.
		)t;

-- 4.Check again for Nulls or Duplicates in Primary Key
SELECT 
cst_id,
COUNT(*)
FROM silver.crm_cust_info
GROUP BY cst_id
HAVING COUNT(*)>1 or cst_id is null;

-- 5.Check again for unwanted Spaces
SELECT cst_lastname
FROM silver.crm_cust_info
WHERE cst_lastname != TRIM(cst_lastname);

-- 6.Check Standardization & Consistency
SELECT DISTINCT cst_gndr
FROM silver.crm_cust_info;

SELECT * FROM silver.crm_cust_info;

/* Clean & Load crm_pro_info. */
-- 1. Check for Nulls or Duplicates in Primary Key
-- Expectation： No Result
SELECT 
prd_id,
COUNT(*)
FROM bronze.crm_prd_info
GROUP BY prd_id
HAVING COUNT(*)>1;

-- 2. Check for unwanted Spaces，Data Standardization & Consistency.
SELECT prd_nm
FROM bronze.crm_prd_info
WHERE prd_nm != TRIM(prd_nm);

SELECT distinct prd_line
FROM bronze.crm_prd_info;

SELECT 
		prd_id,
		prd_key,
		prd_nm,
		prd_start_dt,
		prd_end_dt,
		LEAD (prd_start_dt) OVER(PARTITION BY prd_key ORDER BY prd_start_dt)-1 AS pro_end_test
FROM bronze.crm_prd_info
WHERE prd_start_dt > prd_end_dt;	

-- 3. Clean & Load into the table
PRINT '>> Truncating Table:silver.crm_prd_info';
TRUNCATE TABLE silver.crm_cust_info
PRINT '>> Inserting Data Into:silver.crm_prd_info';
INSERT INTO silver.crm_prd_info(
	prd_id,
	cat_id,
	prd_key,
	prd_nm,
	prd_cost,
	prd_line,
	prd_start_dt,
	prd_end_dt
)	
SELECT	prd_id,
		REPLACE(SUBSTRING(prd_key,1,5),'-','_')AS cat_id,
		SUBSTRING(prd_key,7,len(prd_key)) AS prd_key,
		prd_nm,
		ISNULL(prd_cost,0) AS prd_cost,
		CASE UPPER(TRIM(prd_line))
			 WHEN 'M' THEN 'Mountain'
			 WHEN 'R' THEN 'Road'
			 WHEN 'S' THEN 'Other Sales'
			 WHEN 'T' THEN 'Touring'
			 ELSE 'n/a'
		END AS prd_line,
		CAST(prd_start_dt AS DATE) AS prd_start_dt,
		CAST(LEAD (prd_start_dt) OVER(PARTITION BY prd_key ORDER BY prd_start_dt)-1 AS DATE) AS prd_end_dt
FROM bronze.crm_prd_info;

-- 4.Check again for unwanted Spaces
SELECT prd_nm
FROM silver.crm_prd_info
WHERE prd_nm != TRIM(prd_nm);

-- Check for Nulls or Negative Numbers
SELECT prd_cost
FROM silver.crm_prd_info
WHERE prd_cost < 0 or prd_cost IS NULL;

-- 5.Check Standardization & Consistency
SELECT DISTINCT prd_line
FROM silver.crm_prd_info;

-- Check for Invalid Date Orders
SELECT * 
FROM silver.crm_prd_info
WHERE prd_end_dt < prd_start_dt;

SELECT * 
FROM silver.crm_prd_info;

/* Clean & Load crm_sales_details. */
-- Check for Invalid Dates (e.g., the boundary, the length of date)
SELECT NULLIF(sls_order_dt,0) AS sls_order_dt
FROM bronze.crm_sales_details
WHERE sls_order_dt <= 0 
OR LEN(sls_order_dt) != 8 
OR sls_order_dt > 20500101 
OR sls_order_dt < 19000101;

SELECT NULLIF(sls_ship_dt,0) AS sls_ship_dt
FROM bronze.crm_sales_details
WHERE sls_ship_dt <= 0 
OR LEN(sls_ship_dt) != 8 
OR sls_ship_dt > 20500101 
OR sls_ship_dt < 19000101;

SELECT NULLIF(sls_due_dt,0) AS sls_due_dt
FROM bronze.crm_sales_details
WHERE sls_due_dt <= 0 
OR LEN(sls_due_dt) != 8 
OR sls_due_dt > 20500101 
OR sls_due_dt < 19000101;

SELECT *
FROM bronze.crm_sales_details
WHERE sls_order_dt > sls_ship_dt OR sls_order_dt > sls_due_dt;	

-- Check Data Consistency： Between Sales,Quantity, and Price.
-- 1) Sales = Quantity * Price.
-- 2) Values must not be NULL, Zero, or Negative. 
SELECT sls_sales as old_sales,
       sls_quantity,
	   sls_price as old_price,
	   CASE WHEN sls_sales IS NULL OR sls_sales<=0 OR sls_sales != sls_quantity * ABS(sls_price)
				THEN sls_quantity*ABS(sls_price)
			ELSE sls_sales
	   END AS sls_sales,
	   CASE WHEN sls_price IS NULL OR sls_price <= 0
				THEN ABS(sls_sales) / NULLIF(sls_quantity,0)
			ELSE sls_price
	   END AS sls_price
FROM bronze.crm_sales_details
WHERE	sls_sales != sls_quantity*sls_price 
		OR sls_sales <= 0 OR sls_sales is null 
		OR sls_quantity <= 0 OR sls_quantity is null
		OR sls_price <= 0 OR sls_quantity is null
ORDER BY sls_sales, sls_quantity, sls_price;

--#1 SOLUTION : Data issues will be fixed directly in source system.
--#2 SOLUTION : Data issues has to be fixed in data warehouse.
				-- 2.1 If Sales is negative, null and 0, derive it using Quantity and Price.
				-- 2.2 If Price is null and 0, calculate it using Sales and Quantity.
				-- 2.3 If Price is negative, convert it to a positive value.

PRINT '>> Truncating Table:silver.crm_sales_details';
TRUNCATE TABLE silver.crm_cust_info
PRINT '>> Inserting Data Into:silver.crm_sales_details';
INSERT INTO silver.crm_sales_details(
	sls_ord_num,
	sls_prd_key,
	sls_cust_id,
	sls_order_dt,
	sls_ship_dt,
	sls_due_dt,
	sls_sales,
	sls_quantity,
	sls_price
)
SELECT 
		sls_ord_num,
		sls_prd_key,
		sls_cust_id,
		CASE WHEN sls_order_dt = 0 OR LEN(sls_order_dt) != 8 THEN NULL
		     ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE) 
	    END AS sls_order_dt,
		CASE WHEN sls_ship_dt = 0 OR LEN(sls_ship_dt) != 8 THEN NULL
		     ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE) 
	    END AS sls_ship_dt,
		CASE WHEN sls_due_dt = 0 OR LEN(sls_due_dt) != 8 THEN NULL
		     ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE) 
	    END AS sls_due_dt,
		CASE WHEN sls_sales IS NULL OR sls_sales<=0 OR sls_sales != sls_quantity * ABS(sls_price)
				THEN sls_quantity*ABS(sls_price)
			ELSE sls_sales
		END AS sls_sales,
		sls_quantity,
		CASE WHEN sls_price IS NULL OR sls_price <= 0
				THEN ABS(sls_sales) / NULLIF(sls_quantity,0)
			ELSE sls_price
		END AS sls_price
FROM bronze.crm_sales_details;

-- Check again for validity
SELECT *
FROM silver.crm_sales_details
WHERE sls_order_dt > sls_ship_dt OR sls_order_dt > sls_due_dt;

SELECT sls_sales,
       sls_quantity,
	   sls_price
FROM silver.crm_sales_details
WHERE	sls_sales != sls_quantity*sls_price 
		OR sls_sales <= 0 OR sls_sales is null 
		OR sls_quantity <= 0 OR sls_quantity is null
		OR sls_price <= 0 OR sls_quantity is null
ORDER BY sls_sales, sls_quantity, sls_price;

SELECT *
FROM silver.crm_sales_details;

/* Clean & Load erp_cust_az12 */
-- Check and Identify Out-Of-Range Dates
SELECT BDATE
FROM bronze.erp_cust_az12
WHERE BDATE < '1924-01-01' OR BDATE > GETDATE();

-- Data Standardization & Consistency
SELECT DISTINCT GEN,
	   CASE WHEN UPPER(TRIM(GEN)) IN ('F','FEMALE') THEN 'Female'
	        WHEN UPPER(TRIM(GEN)) IN ('M','MALE')   THEN 'Male'
			ELSE 'n/a'
	   END AS GEN
FROM bronze.erp_cust_az12

PRINT '>> Truncating Table:silver.erp_cust_az12';
TRUNCATE TABLE silver.crm_cust_info
PRINT '>> Inserting Data Into:silver.erp_cust_az12';
INSERT INTO silver.erp_cust_az12(
	CID,
	BDATE,
	GEN
)
SELECT 
       CASE WHEN CID LIKE 'NAS%' THEN SUBSTRING(CID,4,LEN(CID)) -- Remove 'NAS' prefix if present
	        ELSE CID
       END AS CID  
	  ,CASE WHEN BDATE > GETDATE() THEN NULL -- Set future birthdays to NULL
			ELSE BDATE
       END AS BDATE
      ,CASE WHEN UPPER(TRIM(GEN)) IN ('F','FEMALE') THEN 'Female'
	        WHEN UPPER(TRIM(GEN)) IN ('M','MALE')   THEN 'Male'
			ELSE 'n/a'
	   END AS GEN -- Normalize gender values and handle unknown	cases
FROM bronze.erp_cust_az12;

/* Clean & Load erp_loc_a101 */
PRINT '>> Truncating Table:silver.erp_loc_a101';
TRUNCATE TABLE silver.crm_cust_info
PRINT '>> Inserting Data Into:silver.erp_loc_a101';
INSERT INTO silver.erp_loc_a101 (
		CID,
		CNTRY
	)
SELECT replace(CID,'-','') as CID
	  ,CASE WHEN TRIM(CNTRY) = 'DE' THEN 'Germany'
			WHEN TRIM(CNTRY) IN ('US','USA') THEN 'United States'
			WHEN TRIM(CNTRY) = '' OR CNTRY IS NULL THEN 'n/a'
			ELSE TRIM(CNTRY)
		END AS CNTRY
FROM bronze.erp_loc_a101;

-- Data Standardization & Consistency
SELECT DISTINCT CNTRY
FROM silver.erp_loc_a101
ORDER BY CNTRY;

SELECT *
FROM silver.erp_loc_a101;

/* Clean & Load erp_px_cat_g1v2 */
PRINT '>> Truncating Table:silver.erp_px_cat_g1v2';
TRUNCATE TABLE silver.crm_cust_info
PRINT '>> Inserting Data Into:silver.erp_px_cat_g1v2';
INSERT INTO silver.erp_px_cat_g1v2(
	ID,
	CAT,
	SUBCAT,
	MAINTENANCE
)
SELECT 
	ID,
	CAT,
	SUBCAT,
	MAINTENANCE
FROM bronze.erp_px_cat_g1v2;

-- Check for unwanted spaces
SELECT *
FROM bronze.erp_px_cat_g1v2
WHERE CAT != TRIM(CAT) OR SUBCAT != TRIM(SUBCAT) OR MAINTENANCE != TRIM(MAINTENANCE);

-- Data Standardization & Consistency
SELECT DISTINCT CAT
FROM bronze.erp_px_cat_g1v2;

SELECT * FROM silver.erp_px_cat_g1v2;

