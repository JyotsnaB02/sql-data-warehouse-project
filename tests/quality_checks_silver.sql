/*
========================================================================
Quality Checks
========================================================================
Script Purpose:
    This script performs various quality checks for data consistency,accuracy,
    and standardization across the 'silver' schemas.It includes checks for:
    - Null or duplicate primary keys.
    - Unwanted spaces in string fields.
    - Invalid date ranges and orders.
    - Data consistency between related fields.

Usage Notes:
    - Run these checks after data loading Silver Layer.
    - Investigate and resolve any discrepancies found during the checks.
====================================================================
*/

=================================================================
Checking 'silver.crm_cust_info'
=================================================================
--Check For Nulls or Duplicates in Primary Key
--Expectation: No Result

SELECT 
cst_id,
COUNT(*)
FROM silver.crm_cust_info
GROUP BY cst_id
HAVING COUNT(*) >1 OR cst_id IS NULL

-- Check for unwanted Spaces
--Expectation:NO Results
SELECT cst_lastname
FROM silver.crm_cust_info
WHERE cst_lastname != TRIM (cst_lastname) 

--Data Standardization & Consistency
SELECT DISTINCT cst_marital_status
FROM silver.crm_cust_info
SELECT DISTINCT cst_gndr
FROM silver.crm_cust_info


--Check For Nulls or Duplicates in Primary Key
--Expectation: No Result

SELECT 
cst_id,
COUNT(*)
FROM bronze.crm_cust_info
GROUP BY cst_id
HAVING COUNT(*) >1 OR cst_id IS NULL

-- Check for unwanted Spaces
--Expectation:NO Results
SELECT cst_firstname
FROM bronze.crm_cust_info
WHERE cst_firstname != TRIM (cst_firstname) 

--Data Standardization & Consistency
SELECT DISTINCT cst_marital_status
FROM bronze.crm_cust_info
SELECT DISTINCT cst_gndr
FROM bronze.crm_cust_info




================================================================
Checking 'silver.crm_prd_info'
 ===============================================================
--- CHECK FOR Nulls and Duplicates IN PRIMARY KEY
--Expectation :NO Result
SELECT
prd_id,
COUNT(*)
FROM silver.crm_prd_info
GROUP BY prd_id
HAVING COUNT(*) >1 OR prd_id IS NULL

--CHECK FOR UNWANTED Spaces
---EXPECTATION :NO RESulTS
SELECT prd_nm
FROM silver.crm_prd_info
WHERE prd_nm != TRIM(prd_nm)


--Check for NUll and negative numbers
---Excpectation: No Results
SELECT prd_cost
FROM silver.crm_prd_info
WHERE prd_cost <0 OR prd_cost IS NULL

---- Data standardization & consistency
SELECT DISTINCT prd_line
FROM silver.crm_prd_info

----CHECK FOR INVALID DAtE ORDERS
SELECT *
FROM silver.crm_prd_info
WHERE prd_end_dt < prd_start_dt

===================================================================
Checking 'silver.crm_sales_details'
===================================================================
--check null for primary key and White spaces

SELECT * FROM 
silver.crm_sales_details 
WHERE sls_ord_num IS NULL

SELECT * FROM silver.crm_sales_details
WHERE sls_ord_num != TRIM(sls_ord_num)

----check product key same as product table
SELECT * 
FROM silver.crm_sales_details
WHERE sls_prd_key NOT IN(select prd_key FROM silver.crm_prd_info)

----check integrity with customerID
SELECT * 
FROM silver.crm_sales_details
WHERE sls_cust_id NOT IN(select cst_id FROM silver.crm_cust_info)

--- CHECK for Invalid Dates Date cant be zero
SELECT
NULLIF(sls_order_dt,0) sls_order_dt
                                                                                                                                                                                                                                                                                                                                                                                                                                                                           
FROM silver.crm_sales_details
WHERE sls_order_dt <= 0 OR LEN(sls_order_dt) != 8

select 
sls_due_dt
FROM silver.crm_sales_details
WHERE sls_due_dt <=0 OR LEN(sls_due_dt) != 8

---CHECK data Consistency: Between Sales,Quantity,Price
--->> Sales=Quantity * Price
--->> values must not be Null,zero,negative


SELECT
--sls_sales AS old_sls_sales,
sls_quantity,
--sls_price AS old_sls_price,
CASE WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price)
     THEN sls_quantity * ABS(sls_price)
     ELSE sls_sales
     END AS sls_sales,
CASE WHEN sls_price IS NULL OR sls_price <= 0
     THEN ABS(sls_sales / NULLIF( sls_quantity,0))
     ELSE sls_price
     END AS sls_price
FROM silver.crm_sales_details
/*WHERE sls_sales != sls_quantity * sls_price
OR sls_sales IS NULL OR sls_quantity IS NULL OR sls_price IS NULL
OR sls_sales <= 0 OR sls_quantity <= 0 OR sls_price <= 0*/

---FOR sIlver TABLE
--CHECK for inavalid DATE
SELECT *
FROM silver.crm_sales_details
WHERE sls_order_dt >sls_ship_dt OR sls_order_dt > sls_due_dt

---CHECKING for calculations
SELECT DISTINCT
    sls_sales,
    sls_quantity,
    sls_price
 FROM silver.crm_sales_details
 WHERE sls_sales != sls_quantity * sls_price OR
       sls_sales IS NULL OR sls_quantity IS NULL OR sls_price IS NULL
       OR sls_sales <=0 OR sls_quantity <=0 OR sls_price <= 0
ORDER BY sls_sales,sls_quantity,sl

====================================================================
Checking 'silver.erp_cust_az12'
====================================================================
--check for cust_id
  SELECT DISTINCT 
cid
FROM silver.erp_cust_az12
----Data Standardization and Consistency
SELECT DISTINCT
gen
FROM silver.erp_cust_az12
--check for bdate
  ---Identify Out-of-Range Dates
  ---Expectation:Birthdays between 1924.01.01 and Today
SELECT DISTINCT
bdate 
FROM silver.erp_cust_az12
WHERE bdate < '1924-01-01' OR bdate > GETDATE()

====================================================================
Checking 'silver.erp_loc_a101'
====================================================================
Data Standardization and Consistency
SELECT DISTINCT
    cntry
FROM silver.erp_loc_a101
ORDER BY cntry;
===================================================================
Checking 'silver.erp_px_cat_g1v2'
===================================================================
------Check for umwanted spaces
-----Expectation: No Results
  SELECT * FROM silver.erp_px_cat_g1v2
WHERE cat != TRIM(cat) OR 
      TRIM(subcat) != subcat OR 
      maintenance != TRIM(maintenance)







