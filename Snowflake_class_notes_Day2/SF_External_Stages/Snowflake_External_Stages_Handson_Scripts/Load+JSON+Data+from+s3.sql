USE INGEST_DATA;

------------- LOAD JSON DATA -------------
CREATE or replace TABLE HEALTHCARE_JSON(
    id VARCHAR(50)
   ,AVERAGE_COVERED_CHARGES    VARCHAR(150)  
   ,AVERAGE_TOTAL_PAYMENTS    VARCHAR(150)  
   ,TOTAL_DISCHARGES    INTEGER
   ,BACHELORORHIGHER    FLOAT
   ,HSGRADORHIGHER    VARCHAR(150)   
   ,TOTALPAYMENTS    VARCHAR(128)  
   ,REIMBURSEMENT    VARCHAR(128)  
   ,TOTAL_COVERED_CHARGES    VARCHAR(128) 
   ,REFERRALREGION_PROVIDER_NAME    VARCHAR(256)  
   ,REIMBURSEMENTPERCENTAGE    VARCHAR(150)   
   ,DRG_DEFINITION    VARCHAR(256)  
   ,REFERRAL_REGION    VARCHAR(26)  
   ,INCOME_PER_CAPITA    VARCHAR(150)   
   ,MEDIAN_EARNINGSBACHELORS    VARCHAR(150)   
   ,MEDIAN_EARNINGS_GRADUATE    VARCHAR(150) 
   ,MEDIAN_EARNINGS_HS_GRAD    VARCHAR(150)   
   ,MEDIAN_EARNINGSLESS_THAN_HS    VARCHAR(150) 
   ,MEDIAN_FAMILY_INCOME    VARCHAR(150)   
   ,NUMBER_OF_RECORDS    VARCHAR(150)  
   ,POP_25_OVER    VARCHAR(150)  
   ,PROVIDER_CITY    VARCHAR(128)  
   ,PROVIDER_ID    VARCHAR(150)   
   ,PROVIDER_NAME    VARCHAR(256)  
   ,PROVIDER_STATE    VARCHAR(128)  
   ,PROVIDER_STREET_ADDRESS    VARCHAR(256)  
   ,PROVIDER_ZIP_CODE    VARCHAR(150) 
   ,filename    VARCHAR
   ,file_row_number VARCHAR
   ,load_timestamp timestamp default TO_TIMESTAMP_NTZ(current_timestamp)
);

--Create integration object for external stage
create or replace storage integration s3_int_json
  type = external_stage
  storage_provider = s3
  enabled = true
  storage_aws_role_arn = 'arn:aws:iam::696176995234:role/Snowflake-role'
  storage_allowed_locations = ('s3://snowflake-aws-demo1/snowflake/json/');

--create json format
create or replace file format INGEST_DATA.public.json_format
  type = 'json';

--Create external stage object
create or replace stage INGEST_DATA.public.ext_json_stage
  URL = 's3://snowflake-aws-demo1/snowflake/json/'
  STORAGE_INTEGRATION = s3_int_json
  file_format = INGEST_DATA.public.json_format;
  
 LIST INGEST_DATA.public.ext_json_stage;

-- Use copy command to ingest data from S3
copy into INGEST_DATA.public.healthcare_json
from (select 
$1:"_id"::varchar,
$1:" Average Covered Charges "::varchar,
$1:" Average Total Payments "::varchar,
$1:" Total Discharges "::integer,
$1:"% Bachelor's or Higher"::float,
$1:"% HS Grad or Higher"::varchar,
$1:"Total payments"::varchar,
$1:"% Reimbursement"::varchar,
$1:"Total covered charges"::varchar,
$1:"Referral Region Provider Name"::varchar,
$1:"ReimbursementPercentage"::varchar,
$1:"DRG Definition"::varchar,
$1:"Referral Region"::varchar,
$1:"INCOME_PER_CAPITA"::varchar,
$1:"MEDIAN EARNINGS - BACHELORS"::varchar,
$1:"MEDIAN EARNINGS - GRADUATE"::varchar,
$1:"MEDIAN EARNINGS - HS GRAD"::varchar,
$1:"MEDIAN EARNINGS- LESS THAN HS"::varchar,
$1:"MEDIAN_FAMILY_INCOME"::varchar,
$1:"Number of Records"::varchar,
$1:"POP_25_OVER"::varchar,
$1:"Provider City"::varchar,
$1:"Provider Id"::varchar,
$1:"Provider Name"::varchar,
$1:"Provider State"::varchar,
$1:"Provider Street Address"::varchar,
$1:"Provider Zip Code"::varchar,
METADATA$FILENAME,
METADATA$FILE_ROW_NUMBER,
TO_TIMESTAMP_NTZ(current_timestamp)
from @INGEST_DATA.public.ext_json_stage);

select * from healthcare_json;