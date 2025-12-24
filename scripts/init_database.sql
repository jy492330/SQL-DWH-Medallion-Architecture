/*
______________________________________________________
Drop and Recreate Database and Schemas (Fresh Reset)
______________________________________________________

Purpose:
- If database [DWH] exists, it will be dropped and recreated (clean reruns).
- Creates schemas: [bronze], [silver], [gold].

WARNING:
- This script DROPS the entire [DWH] database if it exists.
- All data will be permanently deleted. Ensure backups before running.
*/

-- Switch to system database
USE master;
GO

-- If the new name exists, drop and recreate it (so reruns are clean)
-- IF EXISTS (SELECT 1 FROM sys.databases WHERE name = 'DWH')  -- Alternate, best for filtering with extra conditions
IF DB_ID('DWH') IS NOT NULL
BEGIN
    ALTER DATABASE [DWH] SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
    DROP DATABASE [DWH];
END;
GO

-- Create the 'DWH' database
CREATE DATABASE [DWH];
GO

USE [DWH];
GO

-- Create Schemas
CREATE SCHEMA [bronze];
GO
CREATE SCHEMA [silver];
GO
CREATE SCHEMA [gold];
GO


/*
_________________________________________________________________________________
Create Database and Schemas Only If They Don't Exist (Nothing Will Be Dropped)
_________________________________________________________________________________
*/
-- Create DB if missing
USE master;
GO

IF DB_ID('DWH') IS NULL
BEGIN
    CREATE DATABASE [DWH];
END;
GO

-- Switch to DB
USE [DWH];
GO

-- Create schemas if missing
IF SCHEMA_ID('bronze') IS NULL EXEC('CREATE SCHEMA [bronze]');
IF SCHEMA_ID('silver') IS NULL EXEC('CREATE SCHEMA [silver]');
IF SCHEMA_ID('gold')   IS NULL EXEC('CREATE SCHEMA [gold]');
GO


/*
________________________________________________________________________________________________
“sanity check” if the current query tab is connected to (or set to run in) the master database
________________________________________________________________________________________________

SSMS keeps the “current database” per query window/tab, not globally for all queries in SSMS.
*/
  
SELECT DB_NAME() AS current_db;


/*
__________________________________________________________________
“sanity check” if the schemas are created in the correct database
__________________________________________________________________
*/

USE DWH;
GO

SELECT DB_NAME() AS current_db;

SELECT name
FROM sys.schemas
WHERE name IN ('bronze','silver','gold')
ORDER BY name;
