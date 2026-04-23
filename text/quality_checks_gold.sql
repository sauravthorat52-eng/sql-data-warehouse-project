
/*
---------------------------------------------------------------------------------
Quality Checks
=================================================================================
Script Purpose:
       This script perforas quality ks to validate the integrity, consistency,
       and accuracy of the Gold Layer. These checks ensure:
         -Uniqueness of surrogate keys in dimension tables. 
         -Referential integrity between fact and dimension tables.
         -Validation of relationships in the data model for analytical purposes.

Usage Notes:
      -Run these checks after data loading Silver Layer.
      -Investigate and resolve any discrepancies found during the checks.
=================================================================================
*/

--================================================================
--Checking guld.dim_customer's
--================================================================
--Check for uniqueness of Customer Key in gold.din customers
--Expectation: No results
SELECT
    customer_key,
    COUNT(*) AS duplicate_count
FROM gold.dim_customers
GROUP BY customer_key
HAVING COUNT(*) > 1 ;

--==============================================================================
--Checking 'golds.product_key'
--================================================================
--Check for uniqueness of Customer Key in gold.din customers
--Expectation: No results
SELECT
    customer_key,
    COUNT(*) AS duplicate_count
FROM gold.product_key
GROUP BY product_key
HAVING COUNT(*) > 1 ;
