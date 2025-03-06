/*
===============================================================================
DDL Script: Create Gold Views
===============================================================================
Script Purpose:
    This script creates views for the Gold layer in the data warehouse. 
    The Gold layer represents the final dimension and fact tables (Star Schema)

    Each view performs transformations and combines data from the Silver layer 
    to produce a clean, enriched, and business-ready dataset.

Usage:
    - These views can be queried directly for analytics and reporting.
===============================================================================
*/

----------------------------------------------------------------------------
			 -- CREATE DIMENSION: "gold.dim_customers"
----------------------------------------------------------------------------
IF OBJECT_ID('gold.dim_customers','V') IS NOT NULL
	DROP VIEW  gold.dim_customers;
GO

CREATE VIEW gold.dim_customers as 
SELECT
	  ROW_NUMBER() OVER(ORDER BY cst_id) as customer_key,   --Surrogate Key
	  cst_id				as customer_id,
      cst_key				as customer_number,
      cst_firstname			as first_name,
      cst_lastname			as last_name,
	  ca.BDATE				as birthdate,
	  la.CNTRY				as country,
      CASE WHEN ci.cst_gndr != 'N/A' then ci.cst_gndr  -- CRM is the primary source for gender
		    else coalesce(ca.GEN, 'N/A')			   -- Fallback to ERP data
		end			as gender,
	  cst_marital_status    as martial_status,
      cst_create_date		as create_date
	  
  FROM silver.crm_cust_info ci
  LEFT JOIN silver.erp_cust_az12 ca
  on ci.cst_key = ca.CID
  LEFT JOIN silver.erp_loc_a101 la
  on ci.cst_key = la.CID
GO
----------------------------------------------------------------------------
			 -- CREATE DIMENSION: "gold.dim_products"
----------------------------------------------------------------------------

IF OBJECT_ID('gold.dim_products','V') IS NOT NULL
	DROP VIEW gold.dim_products;
GO

CREATE VIEW gold.dim_products as
select
	ROW_NUMBER() OVER(ORDER BY pr.prd_start_dt) as product_key,  --Surrogate Key
	pr.prd_id		as product_id,
	pr.cat_id		as category_id,
	pr.prd_key		as product_number,
	pr.prd_nm		as product_name,
	pr.prd_cost	    as product_cost,
	pr.prd_line		as product_line,
	pr.prd_start_dt as start_date,
	cg.CAT			as category,
	cg.SUBCAT		as sub_category,
	cg.MAINTENANCE  as maintenance
from silver.crm_prd_info pr
left join silver.erp_px_cat_G1V2 cg
on pr.cat_id = cg.ID
where pr.prd_end_dt is null;    -- Filter out all historical data

GO


----------------------------------------------------------------------------
			 -- CREATE Fact table: "gold.fact_sales"
----------------------------------------------------------------------------
IF OBJECT_ID('gold.fact_sales','V') IS NOT NULL
	DROP VIEW gold.fact_sales;
GO

CREATE VIEW gold.fact_sales as 
select	
	sd.sls_ord_num  as order_number,
	dp.product_key  as product_key,
	dc.customer_key as customer_key,
	sd.sls_order_dt as order_date,
	sd.sls_ship_dt  as ship_date,
	sd.sls_due_dt   as due_date,
	sd.sls_sales    as sales,
	sd.sls_quantity as quantity,
	sd.sls_price    as price
from silver.crm_sales_details sd
left join gold.dim_customers dc on sd.sls_cust_id = dc.customer_id
left join gold.dim_products dp on sd.sls_prd_key = dp.product_number
GO




