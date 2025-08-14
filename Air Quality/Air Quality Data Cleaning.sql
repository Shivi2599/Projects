-- ----------------------------------------
-- DATABASE & TABLE CREATION
-- ----------------------------------------

-- Create a new database for the project
CREATE DATABASE air_quality;
USE air_quality;

-- Create table for NYC Air Quality data
CREATE TABLE nyc_dataset(
`Unique ID`        INT  PRIMARY KEY,
`Indicator ID`     INT,
Name             VARCHAR(70),
Measure          VARCHAR(50),
`Measure Info`   VARCHAR(20),
`Geo Type Name`  VARCHAR(15),
`Geo Join ID`    INT,
`Geo Place Name` VARCHAR(80),
`Time Period`    VARCHAR(20),
`Start_Date`     VARCHAR(20),
Year             INT,
Month            VARCHAR(10),
`Data Value`     FLOAT,
`column`         VARCHAR(10),
columns          VARCHAR(10)
);

-- Dataset View
SELECT * FROM nyc_dataset;

-- ----------------------------------------
-- LOAD & CLEAN RAW DATA
-- ----------------------------------------

-- Load data from CSV (ensure file path matches your MySQL secure-file-priv)
LOAD DATA INFILE "C:\\ProgramData\\MySQL\\MySQL Server 8.0\\Uploads\\Air_Quality.csv"
INTO TABLE nyc_dataset
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

-- Remove unused columns if present
ALTER TABLE nyc_dataset
DROP COLUMN `column`, DROP COLUMN `columns`;

-- ----------------------------------------
-- DATA CLEANING STEPS
-- ----------------------------------------

-- Trim whitespace from text fields
UPDATE nyc_dataset
SET 
  Name = TRIM(Name),
  Measure = TRIM(Measure),
  `Measure Info` = TRIM(`Measure Info`),
  `Geo Type Name` = TRIM(`Geo Type Name`),
  `Geo Place Name` = TRIM(`Geo Place Name`),
  `Time Period` = TRIM(`Time Period`),
  Month = TRIM(Month),
  category = TRIM(category);

-- ----------------------------------------
-- REMOVE DUPLICATES
-- ----------------------------------------

-- Create a de-duplicated version keeping the first (lowest ID) row
CREATE TABLE nyc_dataset_deduped AS
SELECT MIN(`Unique ID`) AS `Unique ID`, Name, Measure, `Measure Info`, `Geo Type Name`, `Geo Place Name`, `Time Period`, `Start_Date`, Year, Month, `Data Value`
FROM nyc_dataset
GROUP BY Name, Measure, `Measure Info`, `Geo Type Name`, `Geo Place Name`, `Time Period`, `Start_Date`, Year, Month, `Data Value`;

-- Replace original with deduplicated version
DROP TABLE nyc_dataset;
RENAME TABLE nyc_dataset_deduped TO nyc_dataset;

-- Fix inconsistent spellings (e.g., PM2.5 vs PM 2.5):
UPDATE nyc_dataset
SET Name = REPLACE(Name, 'PM 2.5', 'PM2.5');
 
 -- ----------------------------------------
-- HANDLE NULL VALUES
-- ----------------------------------------
 
-- Checking Null Values 
SELECT
  SUM(CASE WHEN `Unique ID` IS NULL THEN 1 ELSE 0 END) AS null_unique_id,
  SUM(CASE WHEN Name IS NULL THEN 1 ELSE 0 END) AS null_Name,
  SUM(CASE WHEN Measure IS NULL THEN 1 ELSE 0 END) AS null_Measure,
  SUM(CASE WHEN `Measure Info` IS NULL THEN 1 ELSE 0 END) AS null_Measure_Info,
  SUM(CASE WHEN `Geo Type Name` IS NULL THEN 1 ELSE 0 END) AS null_geo_type_name,
  SUM(CASE WHEN `Geo Place Name` IS NULL THEN 1 ELSE 0 END) AS null_geo_place_name,
  SUM(CASE WHEN `Time Period` IS NULL THEN 1 ELSE 0 END) AS null_time_period,
  SUM(CASE WHEN Start_Date IS NULL THEN 1 ELSE 0 END) AS null_Start_Date,
  SUM(CASE WHEN  Year IS NULL THEN 1 ELSE 0 END) AS null_year,
  SUM(CASE WHEN `Data Value` IS NULL THEN 1 ELSE 0 END) AS null_data_value,
  SUM(CASE WHEN `Month` IS NULL THEN 1 ELSE 0 END) AS null_month
FROM nyc_dataset;

-- Removing NULL values
DELETE FROM nyc_dataset
WHERE 
  Name IS NULL OR
  Measure IS NULL OR
  `Measure Info` IS NULL OR
  `Data Value` IS NULL 
  ;

 -- Temporarily Disable Safe Update Mode
SET SQL_SAFE_UPDATES = 0;  

-- Standardize unit: Convert all variants to 'µg/m3'
UPDATE nyc_dataset
SET `Measure Info` = 'µg/m3'
WHERE `Measure Info` LIKE '%g/m3%';

-- Convert "Number per km2" to "Number" in "Measure" column
UPDATE nyc_dataset
SET `Measure` = 'Number'
WHERE `Measure` = "Number per km2";

-- Convert "number" to "per square km" in "Measure Info" column
UPDATE nyc_dataset
SET `Measure Info` = 'per square Km'
WHERE `Measure Info` = "number";

-- ----------------------------------------
-- ADD CATEGORY COLUMN (FOR FILTERING IN POWER BI)
-- ----------------------------------------

-- Creating column "Category" 
-- Add the column
ALTER TABLE nyc_dataset
ADD COLUMN category VARCHAR(60);

-- Update the column
UPDATE nyc_dataset
SET category = 
  CASE 
    WHEN Name = 'Annual vehicle miles traveled' THEN 'Travel-Related'
    WHEN Name = 'Annual vehicle miles traveled (cars)' THEN 'Travel-Related'
    WHEN Name = 'Annual vehicle miles traveled (trucks)' THEN 'Travel-Related'
    WHEN Name LIKE '%Asthma emergency department%' THEN 'Emergency Department Visits'
    WHEN Name LIKE '%hospitalizations' THEN 'Hospitalizations'
    WHEN Name = 'Deaths due to PM2.5' THEN 'Deaths'
    WHEN Name = "Cardiac and respiratory deaths due to Ozone" THEN "Deaths"
    WHEN Name = 'Boiler Emissions- Total NOx Emissions' THEN 'Pollutant'
    WHEN Name = 'Boiler Emissions- Total PM2.5 Emissions' THEN 'Pollutant'
    WHEN Name = 'Boiler Emissions- Total SO2 Emissions' THEN 'Pollutant'
    WHEN Name = 'Fine particles (PM 2.5)' THEN 'Pollutant'
    WHEN Name = 'Nitrogen dioxide (NO2)' THEN 'Pollutant'
    WHEN Name = 'Outdoor Air Toxics - Benzene' THEN 'Pollutant'
    WHEN Name = 'Outdoor Air Toxics - Formaldehyde' THEN 'Pollutant'
    WHEN Name = 'Ozone (O3)' THEN 'Pollutant'
    ELSE NULL
  END;
  
-- ----------------------------------------
-- DATE CONVERSION & ENHANCEMENTS
-- ----------------------------------------

-- Add a new column 'Date' of type DATE
ALTER TABLE nyc_dataset
ADD COLUMN date DATE;

UPDATE nyc_dataset
SET `Date` = STR_TO_DATE(Start_Date, '%d %M %Y');

ALTER TABLE nyc_dataset
DROP COLUMN Start_Date;

SELECT `Date`
FROM nyc_dataset
LIMIT 10;

-- ----------------------------------------
-- ADD POLLUTANT NAME COLUMN
-- ----------------------------------------

-- Add Pollutant_Name Column
-- Add the new column
ALTER TABLE nyc_dataset 
ADD COLUMN Pollutant_Name VARCHAR(50);

-- Populate Pollutant_Name based on known indicators
UPDATE nyc_dataset
SET Pollutant_Name = 
  CASE
    WHEN Name LIKE '%PM2.5%' THEN 'PM2.5'
    WHEN Name LIKE '%NO2%' THEN 'NO2'
    WHEN Name LIKE '%Ozone%' OR Name LIKE '%O3%' THEN 'Ozone'
    WHEN Name LIKE '%SO2%' THEN 'SO2'
    WHEN Name LIKE '%Benzene%' THEN 'Benzene'
    WHEN Name LIKE '%Formaldehyde%' THEN 'Formaldehyde'
    WHEN Name LIKE '%NOx%' THEN 'NOx'
    ELSE NULL
  END
WHERE category = 'Pollutant';

-- ----------------------------------------
-- FINAL PREVIEW
-- ----------------------------------------

SELECT * FROM nyc_dataset
LIMIT 100;


