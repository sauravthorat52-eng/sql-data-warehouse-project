/*Qualicty check*/


SELECT 
prd_id,
prd_key,
prd_nm,
prd_start_dt,
prd_end_dt,
LEAD (prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt) AS prd_end_dt_test
FROM bronze.crm_prd_info
WHERE prd_key IN ('AC-HE-HL-U509-R','AC-HE-HL-U509')


--Check For Nulls or Duplicates in Primary Key
--Exception: No Result
SELECT
prd_id,
COUNT(*)
FROM bronze.crm_prd_info
GROUP BY prd_id
HAVING COUNT(*) > 1 OR prd_id IS NULL

--Check for Nulls or Negative Numbers
--Expectation : No Results
SELECT prd_cost
FROM bronze.crm_prd_info
WHERE prd_cost < 0 OR prd_cost IS NULL


--Expectation : No Results
SELECT cst_lastname
FROM bronze.crm_cust_info
WHERE cst_lastname != TRIM(cst_lastname)


--Data Standardization & Consistency
SELECT DISTINCT prd_line
FROM bronze.crm_prd_info

--Check for Invalid Date Orders
SELECT *
FROM bronze.crm_prd_info
WHERE prd_end_dt < prd_start_dt



--Data Standarddization & Consistency
SELECT DISTINCT cntry 
FROM silver.erp_loc_a101
ORDER BY cntry

SELECT * FROM silver.erp_loc_a101

--Check For Nulls or Duplicates in Primary Key
--Expectation: No Result
SELECT 
cst_id,
COUNT(*)
FROM silver.crm_cust_info
GROUP BY cst_id
HAVING COUNT (*) > 1 OR cst_id IS NULL

--Check for unwanted Speces
--Expectation : No Results
SELECT cst_firstname
FROM silver.crm_cust_info
WHERE cst_key != TRIM(cst_firstname)

--Data Standardization & Consistency
SELECT DISTINCT cst_material_status
FROM bronze.crm_cust_info

SELECT * FROM silver.crm_cust_info

-- Identify out - of - range Dates

SELECT DISTINCT
bdate
FROM silver.erp_cust_az12
WHERE bdate < '1924-01-01'OR bdate > GETDATE ()

-- Data Standardization & Consistency
SELECT DISTINCT 
gen
FROM silver.erp_cust_az12

SELECT * FROM silver.erp_cust_az12

-- Check for Invalid Dates
SELECT 
NULLIF (sls_order_dt,0) sls_order_dt
FROM bronze.crm_sales_details
WHERE sls_order_dt <= 0
OR LEN(sls_order_dt) !=8
OR (sls_order_dt) > 20500101 
OR sls_order_dt < 19000101

-- Check for Invalid Date Orders
SELECT*
FROM bronze.crm_sales_details
WHERE sls_order_dt  > sls_ship_dt  OR sls_order_dt > sls_due_dt


-- Check data consistancy between Sales, Quantity and Price
SELECT  DISTINCT
sls_sales AS old_sls_sales,
sls_quantity,
sls_price AS old_sls_price,
CASE WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity  * ABS (sls_price)
		THEN sls_quantity * ABS (sls_price)
		ELSE sls_sales
	END AS sls_sales,
CASE WHEN sls_price is NULL OR sls_price <= 0
		THEN sls_sales / NULLIF( sls_quantity , 0)
	ELSE sls_price
END AS sls_price
FROM bronze.crm_sales_details
WHERE sls_sales != sls_quantity * sls_price
OR sls_sales IS NULL OR sls_quantity IS NULL OR sls_price IS NULL
OR sls_sales <= 0 OR sls_quantity <=0 OR sls_price <= 0
ORDER BY sls_sales ,sls_quantity , sls_price
