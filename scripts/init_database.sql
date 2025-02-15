/*
=============================================================
Create Database
=============================================================
Script Purpose:
    This script creates a new database named 'DataWarehouse'. 
    If the database exists, it is dropped and recreated.
	
WARNING:
    Running this script will drop the entire 'DataWarehouse' database if it exists. 
    All data in the database will be permanently deleted. Proceed with caution 
    and ensure you have proper backups before running this script.
*/
-- Create the 'DataWarehouse' database
CREATE DATABASE DataWarehouse;

-- Go to the database
USE DataWarehouse;

-- Check the current database
SELECT DATABASE();
