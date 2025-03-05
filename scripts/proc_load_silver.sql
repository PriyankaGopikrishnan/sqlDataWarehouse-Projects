	/*
===============================================================================
Stored Procedure: Load Silver Layer (Bronze -> Silver)
===============================================================================
Script Purpose:
    This stored procedure performs the ETL (Extract, Transform, Load) process to 
    populate the 'silver' schema tables from the 'bronze' schema.
	Actions Performed:
		- Truncates Silver tables.
		- Inserts transformed and cleansed data from Bronze into Silver tables.
		
Parameters:
    None. 
	  This stored procedure does not accept any parameters or return any values.

Usage Example:
    EXEC silver.load_silver;
===============================================================================
*/

CREATE OR ALTER PROCEDURE silver.load_silver as 
BEGIN
	DECLARE @start_time DATETIME,@end_time DATETIME,@batch_start_time DATETIME, @batch_end_time DATETIME; 
	BEGIN TRY
			SET @batch_start_time = GETDATE();
			PRINT '========================================================'
			PRINT 'Loading Silver Layer'
			PRINT '========================================================'
--------------------------------------------------------------------------------------------------
-- Loading CRM Tables
--------------------------------------------------------------------------------------------------
			PRINT '                                                            '
			PRINT '---------------------Loading CRM Tables---------------------'
			PRINT '                                                            '
			SET @start_time = GETDATE();	
			PRINT '>> Truncating Table : silver.crm_cust_info'
			TRUNCATE TABLE silver.crm_cust_info
			PRINT '>> Inserting Table : silver.crm_cust_info'
			INSERT INTO silver.crm_cust_info(
				cst_id,cst_key,cst_firstname,cst_lastname,cst_marital_status,cst_gndr,cst_create_date)

			SELECT 
				cst_id,
				cst_key,
				TRIM(cst_firstname) as cst_firstname,
				TRIM(cst_lastname) as cst_lastname,
				CASE WHEN cst_marital_status = 'S' then 'single' 
					 WHEN cst_marital_status = 'M' then 'Married'
					 ELSE 'N/A' 
					 END cst_marital_status,    --Normalize martial status values to readable format
				CASE WHEN UPPER(TRIM(cst_gndr)) = 'M' then 'Male'
					 WHEN UPPER(TRIM(cst_gndr)) = 'F' then 'Female'
					 Else 'N/A'
					 END cst_gndr,			    --Normalize gender values to readable format
				cst_create_date
			FROM (
				SELECT *,
					ROW_NUMBER() over (Partition by cst_id order by cst_create_date desc) as flag_list
				FROM bronze.crm_cust_info 
				WHERE cst_id is not null
				)t WHERE flag_list = 1;    -- Select the most recent record per customer

			SET @end_time = GETDATE();
				PRINT '>> Load Duration: '+ cast(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + 'seconds';
				PRINT '----------------------------------------------------------'
--------------------------------------------------------------------------------------------------
			SET @start_time = GETDATE();
			PRINT '>> Truncating Table : silver.crm_prd_info'
			TRUNCATE TABLE silver.crm_prd_info

			PRINT '>> Inserting Table : silver.crm_prd_info'
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

			SELECT prd_id,	
				   REPLACE(SUBSTRING(prd_key,1,5),'-','_') as cat_id,
				   SUBSTRING(prd_key,7,len(prd_key)) as prd_key,
				   prd_nm,
				   ISNULL(prd_cost,0) as prd_cost,
				   CASE UPPER(TRIM(prd_line))
						WHEN 'R' then 'Road'
						WHEN 'S' then 'Other Sales'
						WHEN 'T' then 'Touring'
						WHEN 'M' then 'Mountain'
						ELSE 'N/A'
					END as prd_line,
				   CAST(prd_start_dt as DATE),
				   CAST(LEAD(prd_start_dt) OVER(PARTITION BY prd_key ORDER BY prd_start_dt)-1 as DATE) as prd_end_dt
			FROM bronze.crm_prd_info
			 SET @end_time = GETDATE();
			 PRINT '>> Load Duration: '+ cast(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + 'seconds';
			 PRINT '----------------------------------------------------------'
--------------------------------------------------------------------------------------------------
			SET @start_time = GETDATE();
			PRINT '>> Truncating Table : silver.crm_sales_details'
			TRUNCATE TABLE silver.crm_sales_details
			PRINT '>> Inserting Table : silver.crm_sales_details'
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
			SELECT sls_ord_num,
				  sls_prd_key,
				  sls_cust_id,
				  case when sls_order_dt <=0 OR LEN(sls_order_dt) != 8  then NULL
					   else CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)
				  end as sls_order_dt,
				  case when sls_ship_dt <=0 OR LEN(sls_order_dt) != 8  then NULL
					   else CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE)
				  end as sls_ship_dt,
				  case when sls_due_dt <=0 OR LEN(sls_order_dt) != 8  then NULL
					   else CAST(CAST(sls_due_dt AS VARCHAR) AS DATE)
				  end as sls_due_dt,
				  CASE WHEN sls_sales IS NULL OR sls_sales!= sls_quantity*ABS(sls_price)  OR sls_sales <=0 
							THEN sls_quantity*ABS(sls_price)
					   ELSE sls_sales    
				  END AS sls_sales,		  -- Recalculate sales if original value is missing or incorrect
				 sls_quantity,
				 CASE WHEN sls_price IS NULL OR sls_price < =0 
						THEN sls_sales/sls_quantity
					  ELSE sls_price
				  END AS sls_price         -- Derive price if original value is invalid
			  FROM bronze.crm_sales_details
			SET @end_time = GETDATE();
			PRINT '>> Load Duration: '+ cast(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + 'seconds';
			PRINT '----------------------------------------------------------'
--------------------------------------------------------------------------------------------------
	-- Loading ERP tables
--------------------------------------------------------------------------------------------------
			PRINT '                                                            '
			PRINT '---------------------Loading ERP Tables---------------------'
			PRINT '                                                            '
			SET @start_time = GETDATE();
			PRINT '>> Inserting Table : silver.erp_cust_az12'
			TRUNCATE TABLE silver.erp_cust_az12
			PRINT '>> Inserting Table : silver.erp_cust_az12'
	
			INSERT INTO silver.erp_cust_az12(
					CID,
					BDATE,
					GEN
				)
			Select 
				CASE WHEN CID LIKE 'NAS%' THEN SUBSTRING(CID,4,LEN(CID))    -- Remove 'NAS' prefix if present
					ELSE CID
				END AS CID,
				CASE WHEN BDATE > GETDATE() THEN NULL
					 ELSE BDATE
				END AS BDATE,   -- Set future birthdates to NULL
				CASE WHEN UPPER(TRIM(GEN)) IN ('F','FEMALE') THEN 'Female'
					 WHEN UPPER(TRIM(GEN)) IN ('M','MALE') THEN 'Male'
					 ELSE 'N/A'
				END as  GEN    -- Normalize gender values and handle unknown cases
			from bronze.erp_cust_az12
			SET @end_time = GETDATE();
			PRINT '>> Load Duration: '+ cast(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + 'seconds';
			PRINT '----------------------------------------------------------'
			SET @batch_end_time = GETDATE();
--------------------------------------------------------------------------------------------------

			SET @start_time = GETDATE();
			PRINT '>> Truncating Table : silver.erp_loc_a101'
			TRUNCATE TABLE silver.erp_loc_a101
			PRINT '>> Inserting Table : silver.erp_loc_a101'
			INSERT INTO silver.erp_loc_a101(CID,CNTRY)
			select	
					REPLACE(CID,'-',''),
				case when Upper(TRIM(CNTRY)) = 'DE' then 'Germany'
					 when Upper(TRIM(CNTRY)) IN('US','USA') then 'United States'
					 when TRIM(CNTRY) ='' OR CNTRY IS  NULL  then 'N/A'
					 Else TRIM(CNTRY)
				END as CNTRY 
			from bronze.erp_loc_a101
			SET @end_time = GETDATE();
			PRINT '>> Load Duration: '+ cast(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + 'seconds';
			PRINT '----------------------------------------------------------'
--------------------------------------------------------------------------------------------------
			SET @start_time = GETDATE();
			PRINT '>> Truncating Table : silver.erp_px_cat_G1V2'
			TRUNCATE TABLE silver.erp_px_cat_G1V2 
			PRINT '>> Inserting Table : silver.erp_px_cat_G1V2 '
			INSERT INTO silver.erp_px_cat_G1V2 (
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
			SET @end_time = GETDATE();
			PRINT '>> Load Duration: '+ cast(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + 'seconds';
			PRINT '----------------------------------------------------------'
-------------------------------------------------------------------------------------------------
			
			PRINT '========================================================'
			PRINT 'Loading Silver Layer Completed'
			PRINT 'Total Load Duration : ' + cast(DATEDIFF(second,@batch_start_time,@batch_end_time) AS NVARCHAR) + ' seconds';
			PRINT '========================================================'
		END TRY
		BEGIN CATCH
			PRINT '========================================================'
			PRINT 'Error Message: '+ Error_message();
			PRINT 'Error Number' + CAST (ERROR_NUMBER() AS NVARCHAR);
			PRINT 'Error state' + CAST (ERROR_STATE() AS NVARCHAR);
			PRINT '========================================================'
		END CATCH
END;
