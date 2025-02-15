/*
===============================================================================
Load Bronze Layer (Source -> Bronze)
===============================================================================
Script Purpose:
    This stored procedure loads data into the 'bronze' schema from external CSV files. 
    It performs the following actions:
    - Load data from csv Files to bronze tables.

Parameters:
    None. 
	  This stored procedure does not accept any parameters or return any values.
*/
USE DataWarehouse;
SELECT DATABASE();

SHOW VARIABLES LIKE 'local_infile';
LOAD DATA LOCAL
INFILE '/Users/harsh/Desktop/sql-data-warehouse-project/datasets/cust_info.csv'
INTO TABLE bronze_crm_cust_info
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

SHOW VARIABLES LIKE 'local_infile';
LOAD DATA LOCAL
INFILE '/Users/harsh/Desktop/sql-data-warehouse-project/datasets/prd_info.csv'
INTO TABLE bronze_crm_prd_info
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

SHOW VARIABLES LIKE 'local_infile';
LOAD DATA LOCAL
INFILE '/Users/harsh/Desktop/sql-data-warehouse-project/datasets/sales_details.csv'
INTO TABLE bronze_crm_sales_details
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

SHOW VARIABLES LIKE 'local_infile';
LOAD DATA LOCAL
INFILE '/Users/harsh/Desktop/sql-data-warehouse-project/datasets/CUST_AZ12.csv'
INTO TABLE bronze_erp_cust_az12
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

SHOW VARIABLES LIKE 'local_infile';
LOAD DATA LOCAL
INFILE '/Users/harsh/Desktop/sql-data-warehouse-project/datasets/LOC_A101.csv'
INTO TABLE bronze_erp_loc_a101
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

SHOW VARIABLES LIKE 'local_infile';
LOAD DATA LOCAL
INFILE '/Users/harsh/Desktop/sql-data-warehouse-project/datasets/PX_CAT_G1V2.csv'
INTO TABLE bronze_erp_px_cat_g1v2
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

