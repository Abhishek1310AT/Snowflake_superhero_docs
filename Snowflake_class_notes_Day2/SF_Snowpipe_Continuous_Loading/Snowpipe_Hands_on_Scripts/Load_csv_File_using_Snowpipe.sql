USE INGEST_DATA;

-- Create table to load CSV data
CREATE or replace TABLE HEALTHCARE_CSV(
    AVERAGE_COVERED_CHARGES    NUMBER(38,6)  
   ,AVERAGE_TOTAL_PAYMENTS    NUMBER(38,6)  
   ,TOTAL_DISCHARGES    NUMBER(38,0)  
   ,BACHELORORHIGHER    NUMBER(38,1)  
   ,HSGRADORHIGHER    NUMBER(38,1)  
   ,TOTALPAYMENTS    VARCHAR(128)  
   ,REIMBURSEMENT    VARCHAR(128)  
   ,TOTAL_COVERED_CHARGES    VARCHAR(128) 
   ,REFERRALREGION_PROVIDER_NAME    VARCHAR(256)  
   ,REIMBURSEMENTPERCENTAGE    NUMBER(38,9)  
   ,DRG_DEFINITION    VARCHAR(256)  
   ,REFERRAL_REGION    VARCHAR(26)  
   ,INCOME_PER_CAPITA    NUMBER(38,0)  
   ,MEDIAN_EARNINGSBACHELORS    NUMBER(38,0)  
   ,MEDIAN_EARNINGS_GRADUATE    NUMBER(38,0)  
   ,MEDIAN_EARNINGS_HS_GRAD    NUMBER(38,0)  
   ,MEDIAN_EARNINGSLESS_THAN_HS    NUMBER(38,0)  
   ,MEDIAN_FAMILY_INCOME    NUMBER(38,0)  
   ,NUMBER_OF_RECORDS    NUMBER(38,0)  
   ,POP_25_OVER    NUMBER(38,0)  
   ,PROVIDER_CITY    VARCHAR(128)  
   ,PROVIDER_ID    NUMBER(38,0)  
   ,PROVIDER_NAME    VARCHAR(256)  
   ,PROVIDER_STATE    VARCHAR(128)  
   ,PROVIDER_STREET_ADDRESS    VARCHAR(256)  
   ,PROVIDER_ZIP_CODE    NUMBER(38,0)  
);

SELECT * FROM HEALTHCARE_CSV;

--Create integration object for external stage
create or replace storage integration s3_int_csv
  type = external_stage
  storage_provider = s3
  enabled = true
  storage_aws_role_arn = 'arn:aws:iam::696176995234:role/Snowflake-role'
  storage_allowed_locations = ('s3://snowflake-aws-demo1/snowflake/csv/');

--Describe integration object to fetch external_id and to be used in s3
DESC INTEGRATION s3_int;

create or replace file format ingest_data.public.csv_format
                    type = csv
                    --field_delimiter = '|'
                    skip_header = 1
                    null_if = ('NULL', 'null')
                    empty_field_as_null = true;
                    
create or replace stage ingest_data.public.ext_csv_stage
  --URL = 's3://snowflake-aws-demo1/snowflake/csv/health.csv'
  URL = 's3://snowflake-aws-demo1/snowflake/csv/health_pipe.csv'
  STORAGE_INTEGRATION = s3_int_csv
  file_format = ingest_data.public.csv_format;

-- Use copy command to ingest data from S3
copy into healthcare_csv
from @ingest_data.public.ext_csv_stage
on_error = CONTINUE;

-- Use copy command to ingest data from S3 - SNOW PIPE to automate the process
create or replace pipe ingest_data.public.healthcare_pipe_csv auto_ingest=true as
copy into ingest_data.public.healthcare_csv
from @ingest_data.public.ext_csv_stage
on_error = CONTINUE;

select * from healthcare_csv;
truncate table healthcare_csv;

show pipes;

select * from training_db.public.healthcare_json;

truncate table training_db.public.emp_basic;

alter pipe healthcare_pipe_json refresh;


-- Validate

select SYSTEM$PIPE_STATUS('healthcare_pipe_json')

select * from table(validate_pipe_load(
  pipe_name=>'training_db.public.healthcare_pipe_json',
  start_time=>dateadd(hour, -4, current_timestamp())));
  
  