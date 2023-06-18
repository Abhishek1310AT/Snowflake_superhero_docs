use training_db;

truncate table HEALTHCARE_PARQUET;

------------- LOAD PARQUET DATA -------------
CREATE or replace TABLE HEALTHCARE_PARQUET(
    AVERAGE_COVERED_CHARGES    VARCHAR(150)  
   ,AVERAGE_TOTAL_PAYMENTS    VARCHAR(150)  
   ,TOTAL_DISCHARGES    VARCHAR(150)   
   ,BACHELORORHIGHER    VARCHAR(150)   
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

create or replace storage integration s3_int_parquet
  type = external_stage
  storage_provider = s3
  enabled = true
  storage_aws_role_arn = 'arn:aws:iam::443852188086:role/Snowflake-role'
  storage_allowed_locations = ('s3://snowflake-demo-test-26022022/parquet/');
  
  DESC INTEGRATION s3_int_parquet;

--create parquet format
create or replace file format training_db.public.parquet_format
  type = 'parquet';

--Create external stage object
create or replace stage training_db.public.ext_parquet_stage
  URL = 's3://snowflake-demo-test-26022022/parquet/'
  STORAGE_INTEGRATION = s3_int_parquet
  file_format = training_db.public.parquet_format;

copy into healthcare_parquet
from @training_db.public.ext_parquet_stage;

-- Use copy command to ingest data from S3
create or replace pipe training_db.public.healthcare_pipe_parquet auto_ingest=true as
copy into training_db.public.healthcare_parquet
from (select 
$1:average_covered_charges::varchar,
$1:average_total_payments::varchar,
$1:total_discharges::varchar,
$1:"PERCENT_Bachelor's_or_Higher"::varchar,
$1:percent_hs_grad_or_higher::varchar,
$1:total_payments::varchar,
$1:percent_reimbursement::varchar,
$1:total_covered_charges::varchar,
$1:referral_region_provider_name::varchar,
$1:reimbursement_percentage::varchar,
$1:drg_definition::varchar,
$1:referral_region::varchar,
$1:income_per_capita::varchar,
$1:median_earnings_bachelors::varchar,
$1:median_earnings_graduate::varchar,
$1:median_earnings_hs_grad::varchar,
$1:median_earnings_less_than_hs::varchar,
$1:median_family_income::varchar,
$1:number_of_records::varchar,
$1:pop_25_over::varchar,
$1:provider_city::varchar,
$1:provider_id::varchar,
$1:provider_name::varchar,
$1:provider_state::varchar,
$1:provider_street_address::varchar,
$1:provider_zip_code::varchar,
METADATA$FILENAME,
METADATA$FILE_ROW_NUMBER,
TO_TIMESTAMP_NTZ(current_timestamp)
from @training_db.public.ext_parquet_stage);



show pipes;

select * from training_db.public.healthcare_parquet;

alter pipe healthcare_pipe_parquet refresh;


-- Validate

select SYSTEM$PIPE_STATUS('healthcare_pipe_parquet')

select * from table(validate_pipe_load(
  pipe_name=>'training_db.public.healthcare_pipe_parquet',
  start_time=>dateadd(hour, -4, current_timestamp())));



select * from healthcare_parquet;