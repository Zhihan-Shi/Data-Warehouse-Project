/*
Quality Check
================================================================
Script Purpose:
	ThiS script performs various quality checks for data consistency, accuracy, 
	and standardization across the 'silver'schema. It contains checks for:
	- NUll or duplicate primary keys.
    - Unwanted spaces in string fields.
    - Data standardization and consistency.
    - Invalid date ranges and orders.
    - Data consistency between related fields
================================================================
*/

/* Check 'silver.crm_cust_info'. */
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

-- 4.After Loading Finished, Check again for Nulls or Duplicates in Primary Key
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

/* Check 'silver.crm_pro_info'. */
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

-- 3. After loading finished, Check again for unwanted Spaces
	SELECT prd_nm
	FROM silver.crm_prd_info
	WHERE prd_nm != TRIM(prd_nm);

-- 4. Check for Nulls or Negative Numbers
	SELECT prd_cost
	FROM silver.crm_prd_info
	WHERE prd_cost < 0 or prd_cost IS NULL;

-- 5. Check Standardization & Consistency
	SELECT DISTINCT prd_line
	FROM silver.crm_prd_info;

-- 6. Check for Invalid Date Orders
	SELECT * 
	FROM silver.crm_prd_info
	WHERE prd_end_dt < prd_start_dt;
	
	SELECT * 
	FROM silver.crm_prd_info;

/* Check 'silver.crm_sales_details'. */
-- 1. Check for Invalid Dates (e.g., the boundary, the length of date)
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

-- 2. Check Data Consistency： Between Sales,Quantity, and Price.
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

-- 3. After loading finisged, Check again for validity
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

/* Check silver.erp_cust_az12 */
-- 1. Check and Identify Out-Of-Range Dates
	SELECT BDATE
	FROM bronze.erp_cust_az12
	WHERE BDATE < '1924-01-01' OR BDATE > GETDATE();

-- 2. Data Standardization & Consistency
	SELECT DISTINCT GEN,
		   CASE WHEN UPPER(TRIM(GEN)) IN ('F','FEMALE') THEN 'Female'
		        WHEN UPPER(TRIM(GEN)) IN ('M','MALE')   THEN 'Male'
				ELSE 'n/a'
		   END AS GEN
	FROM bronze.erp_cust_az12

/* Check silver.erp_loc_a101 */
-- 1. Data Standardization & Consistency
	SELECT DISTINCT CNTRY
	FROM silver.erp_loc_a101
	ORDER BY CNTRY;
	
	SELECT *
	FROM silver.erp_loc_a101;

/* Check erp_px_cat_g1v2 */
-- 1. Check for unwanted spaces
	SELECT *
	FROM bronze.erp_px_cat_g1v2
	WHERE CAT != TRIM(CAT) OR SUBCAT != TRIM(SUBCAT) OR MAINTENANCE != TRIM(MAINTENANCE);

-- 2. Data Standardization & Consistency
	SELECT DISTINCT CAT
	FROM bronze.erp_px_cat_g1v2;

	SELECT * FROM silver.erp_px_cat_g1v2;

