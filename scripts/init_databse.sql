/*
====================================
Create Databse and Schemas
====================================
Script Purpose:

	This Script creates a new database called 'DataWarehouse' after checking if it already exists.
	If the database exists, it will be dropped and recreated.
	Additionally the script sets up three schemas with in database: 'bronze','silver' and 'gold'.
	
Warning:
	
	This scipt will drop the entire 'DataWarehouse' database if it exists.
	All data in the databse will be permenantely deleted.Proceed with proper caution 
	and ensure you have proper backups before running the script.

*/

USE master;
GO

--Drop and Recreate ''DataWarehouse' Database:

IF EXISTS (SELECT 1 FROM sys.databases where name='DataWarehouse')
BEGIN
	ALTER DATABASE DataWarehouse SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
	DROP DATABASE DataWarehouse;
END;

--Create the 'DataWarehouse' Database:

CREATE DATABASE DataWarehouse;
GO

USE DataWarehouse;
GO

--Create Schemas:

CREATE SCHEMA bronze;

GO

CREATE SCHEMA silver;

GO

CREATE SCHEMA gold;

GO
