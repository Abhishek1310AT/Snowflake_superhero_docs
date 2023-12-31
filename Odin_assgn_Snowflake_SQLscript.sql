
-------------------------------------------------------------
-------------------------------------------------------------
-- 10/01/2023 	WEEK 1 - Assignment


-- create different WAREHOUSES  -----------------------------------------------
CREATE OR REPLACE WAREHOUSE IMPORT_WH WITH WAREHOUSE_SIZE = 'MEDIUM' 
  WAREHOUSE_TYPE = 'STANDARD' 
  AUTO_SUSPEND = 300 
  AUTO_RESUME = TRUE
  INITIALLY_SUSPENDED = TRUE
  MIN_CLUSTER_COUNT = 1 
//  MAX_CLUSTER_COUNT = 1 SCALING_POLICY = 'STANDARD'
  COMMENT = 'use to import data from cloud';
  
CREATE OR REPLACE WAREHOUSE TRANSFORM_WH WITH WAREHOUSE_SIZE = 'MEDIUM' 
  WAREHOUSE_TYPE = 'STANDARD' 
  AUTO_SUSPEND = 300 
  AUTO_RESUME = TRUE 
  INITIALLY_SUSPENDED = TRUE
  MIN_CLUSTER_COUNT = 1 
//  MAX_CLUSTER_COUNT = 1 SCALING_POLICY = 'STANDARD'
  COMMENT = 'use to transform data';

CREATE OR REPLACE WAREHOUSE REPORTING_WH WITH WAREHOUSE_SIZE = 'SMALL' 
  WAREHOUSE_TYPE = 'STANDARD' 
  AUTO_SUSPEND = 900 
  AUTO_RESUME = TRUE 
  INITIALLY_SUSPENDED = TRUE
  MIN_CLUSTER_COUNT = 1 
//  MAX_CLUSTER_COUNT = 5 SCALING_POLICY = 'STANDARD'
  COMMENT = 'use for analysis by data analytics';


-- create DATABASES  -----------------------------------------------
CREATE DATABASE STAGING;
CREATE DATABASE PROD;


-- create SCHEMAS  -----------------------------------------------
CREATE SCHEMA STAGING.RAW;
CREATE SCHEMA STAGING.CLEAN;
CREATE SCHEMA PROD.REPORTING;


-- create ROLES  -----------------------------------------------
CREATE ROLE "IMPORT_ROLE";
GRANT ROLE "USERADMIN" TO ROLE "IMPORT_ROLE";
GRANT USAGE ON WAREHOUSE IMPORT_WH TO ROLE "IMPORT_ROLE";
GRANT USAGE ON DATABASE STAGING TO ROLE "IMPORT_ROLE";           //grant usage of STAGING database
GRANT USAGE ON SCHEMA RAW TO ROLE "IMPORT_ROLE";                 //grant access to SCHEMA.RAW
GRANT USAGE ON SCHEMA CLEAN TO ROLE "IMPORT_ROLE";               //grant access to SCHEMA.CLEAN
GRANT SELECT ON FUTURE TABLES IN SCHEMA STAGING.RAW TO ROLE "IMPORT_ROLE"; 
GRANT SELECT ON FUTURE TABLES IN SCHEMA STAGING.CLEAN TO ROLE "IMPORT_ROLE"; 

SHOW GRANTS TO ROLE IMPORT_ROLE;

CREATE ROLE "TRANSFORM_ROLE";
GRANT USAGE ON WAREHOUSE TRANSFORM_WH TO ROLE "TRANSFORM_ROLE";
GRANT USAGE ON DATABASE STAGING TO ROLE "TRANSFORM_ROLE";              //grant usage of database STAGING
GRANT USAGE ON SCHEMA CLEAN TO ROLE "TRANSFORM_ROLE";                  //grant access to schema STAGING.CLEAN
GRANT SELECT ON FUTURE TABLES IN SCHEMA STAGING.CLEAN TO ROLE "TRANSFORM_ROLE"; 

GRANT USAGE ON DATABASE PROD TO ROLE TRANSFORM_ROLE;
GRANT USAGE ON SCHEMA REPORTING TO ROLE TRANSFORM_ROLE;
GRANT SELECT ON FUTURE TABLES IN SCHEMA PROD.REPORTING TO ROLE TRANSFORM_ROLE;

SHOW GRANTS TO ROLE TRANSFORM_ROLE;

CREATE ROLE REPORTING_ROLE;
GRANT USAGE ON WAREHOUSE REPORTING_WH TO ROLE REPORTING_ROLE;
GRANT USAGE ON DATABASE PROD TO ROLE REPORTING_ROLE;              //grant usage of STAGING database
GRANT USAGE ON SCHEMA REPORTING TO ROLE REPORTING_ROLE;                  //grant access to SCHEMA.RAW
GRANT SELECT ON FUTURE TABLES IN SCHEMA PROD.REPORTING TO ROLE REPORTING_ROLE; 

SHOW GRANTS TO ROLE REPORTING_ROLE;


-- create USERS  -----------------------------------------------
CREATE OR REPLACE USER USER_IMPORT;
GRANT ROLE IMPORT_ROLE TO USER USER_IMPORT;

CREATE OR REPLACE USER USER_TRANSFORM;
GRANT ROLE TRANSFORM_ROLE TO USER USER_TRANSFORM;

CREATE OR REPLACE USER USER_REPORTING;
GRANT ROLE REPORTING_ROLE TO USER USER_REPORTING;


-- create MONITORS  -----------------------------------------------
grant monitor usage on account to role custom;


CREATE OR REPLACE RESOURCE MONITOR "IMPORT_MONITOR" 
 WITH CREDIT_QUOTA = 100, 
 frequency = 'MONTHLY', 
 start_timestamp = 'IMMEDIATELY', 
 end_timestamp = null  
 TRIGGERS ON 50 PERCENT DO NOTIFY
          ON 100 PERCENT DO SUSPEND_IMMEDIATE;
ALTER ACCOUNT SET RESOURCE_MONITOR = "IMPORT_MONITOR";


CREATE OR REPLACE RESOURCE MONITOR "TRANSFORM_MONITOR" 
 WITH CREDIT_QUOTA = 100, 
 frequency = 'MONTHLY', 
 start_timestamp = 'IMMEDIATELY', 
 end_timestamp = null  
 TRIGGERS ON 50 PERCENT DO NOTIFY
          ON 100 PERCENT DO SUSPEND_IMMEDIATE;
ALTER ACCOUNT SET RESOURCE_MONITOR = "TRANSFORM_MONITOR";


CREATE OR REPLACE RESOURCE MONITOR "REPORTING_MONITOR" 
 WITH CREDIT_QUOTA = 100, 
 frequency = 'MONTHLY', 
 start_timestamp = 'IMMEDIATELY', 
 end_timestamp = null  
 TRIGGERS ON 50 PERCENT DO NOTIFY
          ON 100 PERCENT DO SUSPEND_IMMEDIATE;
ALTER ACCOUNT SET RESOURCE_MONITOR = "REPORTING_MONITOR";





