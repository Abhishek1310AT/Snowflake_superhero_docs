CREATE OR REPLACE DATABASE INGEST_DATA;

USE INGEST_DATA;

------------- LOAD JSON DATA -------------
--Raw Source Table
CREATE or replace TABLE HEALTHCARE_RAW(
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

select * from HEALTHCARE_RAW;

-- Final Target Table
CREATE or replace TABLE HEALTHCARE(
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

select * from HEALTHCARE;

--Create integration object for external stage
create or replace storage integration s3_raw_int
  type = external_stage
  storage_provider = s3
  enabled = true
  storage_aws_role_arn = 'arn:aws:iam::712471877095:role/Snowflake-role'
  storage_allowed_locations = ('s3://snowflake-aws-demos/snowflake/csv/','s3://snowflake-aws-demo3/snowflake/json/','s3://snowflake-aws-demo3/snowflake/parquet/'); 
  
--Describe integration object to fetch external_id and to be used in s3
DESC INTEGRATION s3_raw_int;

--create csv format
create or replace file format ingest_data.public.csv_format
                    type = csv
                    --field_delimiter = '|'
                    skip_header = 1
                    null_if = ('NULL', 'null')
                    empty_field_as_null = true;
					
create or replace file format ingest_data.public.json_format
                    type = 'json';
                    --field_delimiter = '|'
                    skip_header = 1
                    null_if = ('NULL', 'null')
                    empty_field_as_null = true;

--Create external stage object
create or replace stage ingest_data.public.ext_stage
  URL = 's3://snowflake-aws-demos/snowflake/csv/'
  STORAGE_INTEGRATION = s3_raw_int
  file_format = ingest_data.public.raw_format;
  
  list @ingest_data.public.ext_stage;
  
  
-- Use copy command to ingest data from S3
/* create or replace pipe ingest_data.public.healthcare_pipe_raw auto_ingest=true as
copy into healthcare_raw
from (SELECT *,METADATA$FILENAME,
METADATA$FILE_ROW_NUMBER,
TO_TIMESTAMP_NTZ(current_timestamp)
from @ingest_data.public.ext_json_stage)
on_error = CONTINUE; */

--show pipes;
  


-- Use copy command to ingest data from S3
/*create or replace pipe ingest_data.public.healthcare_pipe_raw auto_ingest=true as
copy into ingest_data.public.healthcare_json
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
from @ingest_data.public.ext_json_stage);

--list @ingest_data.public.ext_json_stage;

--remove @ingest_data.public.ext_json_stage;*/

-- Use copy command to ingest data from S3
create or replace pipe ingest_data.public.healthcare_pipe_raw auto_ingest=true as
copy into ingest_data.public.healthcare_raw
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
from @ingest_data.public.ext_stage); 
on_error = CONTINUE;

show pipes;

select * from ingest_data.public.healthcare_raw;

alter pipe healthcare_pipe_raw refresh;


-- Validate

select SYSTEM$PIPE_STATUS('healthcare_pipe_raw')

select * from table(validate_pipe_load(
  pipe_name=>'ingest_data.public.healthcare_pipe_raw',
  start_time=>dateadd(hour, -4, current_timestamp())));

select * from healthcare_raw;

truncate healthcare_raw;
drop table healthcare_raw;



--select * from healthcare_csv;
--select * from healthcare_parquet;
--select * from healthcare_json;


-- Create a stream object
create or replace stream ingest_data.public.healthcare_stream on table healthcare_raw;

show streams;


select * from ingest_data.public.healthcare_raw;
select * from ingest_data.public.healthcare_stream;
select * from ingest_data.public.healthcare;


CREATE OR REPLACE PROCEDURE ingest_data.public.upload_healthcare_data()
RETURNS STRING
LANGUAGE JAVASCRIPT
EXECUTE AS OWNER
AS
$$
var rs=snowflake.execute({sqlText:`MERGE INTO ingest_data.public.healthcare F USING ingest_data.public.healthcare_stream S ON F.id=S.id 
WHEN MATCHED AND S.METADATA$ACTION='DELETE' AND S.METADATA$ISUPDATE='FALSE' THEN DELETE WHEN MATCHED AND S.METADATA$ACTION='INSERT' AND S.METADATA$ISUPDATE='TRUE' THEN UPDATE 
SET 
f.id=s.id,
f.AVERAGE_COVERED_CHARGES=s.AVERAGE_COVERED_CHARGES,
f.AVERAGE_TOTAL_PAYMENTS=s.AVERAGE_TOTAL_PAYMENTS,
f.TOTAL_DISCHARGES=s.TOTAL_DISCHARGES,
f.BACHELORORHIGHER = s.BACHELORORHIGHER,
f.HSGRADORHIGHER = s.HSGRADORHIGHER,
f.TOTALPAYMENTS = s.TOTALPAYMENTS,
f.REIMBURSEMENT = s.REIMBURSEMENT,
f.TOTAL_COVERED_CHARGES = s.TOTAL_COVERED_CHARGES,
f.REFERRALREGION_PROVIDER_NAME = s.REFERRALREGION_PROVIDER_NAME,
f.REIMBURSEMENTPERCENTAGE = s.REIMBURSEMENTPERCENTAGE,
f.DRG_DEFINITION = s.DRG_DEFINITION,
f.REFERRAL_REGION = s.REFERRAL_REGION,
f.INCOME_PER_CAPITA = s.INCOME_PER_CAPITA,
f.MEDIAN_EARNINGSBACHELORS = s.MEDIAN_EARNINGSBACHELORS,
f.MEDIAN_EARNINGS_GRADUATE = s.MEDIAN_EARNINGS_GRADUATE,
f.MEDIAN_EARNINGS_HS_GRAD =s .MEDIAN_EARNINGS_HS_GRAD,
f.MEDIAN_EARNINGSLESS_THAN_HS = s.MEDIAN_EARNINGSLESS_THAN_HS,
f.MEDIAN_FAMILY_INCOME = s.MEDIAN_FAMILY_INCOME,
f.NUMBER_OF_RECORDS = s.NUMBER_OF_RECORDS,
f.POP_25_OVER = s.POP_25_OVER,
f.PROVIDER_CITY = s.PROVIDER_CITY,
f.PROVIDER_ID = s.PROVIDER_ID,
f.PROVIDER_NAME = s.PROVIDER_NAME,
f.PROVIDER_STATE = s.PROVIDER_STATE,
f.PROVIDER_STREET_ADDRESS = s.PROVIDER_STREET_ADDRESS,
f.PROVIDER_ZIP_CODE = s.PROVIDER_ZIP_CODE,
f.filename = s.filename,
f.file_row_number = s.file_row_number,
f.load_timestamp = s.load_timestamp
WHEN NOT MATCHED AND S.METADATA$ACTION ='INSERT' THEN 
INSERT(id,AVERAGE_COVERED_CHARGES,AVERAGE_TOTAL_PAYMENTS,TOTAL_DISCHARGES,BACHELORORHIGHER,HSGRADORHIGHER,TOTALPAYMENTS,REIMBURSEMENT,TOTAL_COVERED_CHARGES,REFERRALREGION_PROVIDER_NAME,REIMBURSEMENTPERCENTAGE,DRG_DEFINITION,REFERRAL_REGION,INCOME_PER_CAPITA,MEDIAN_EARNINGSBACHELORS,MEDIAN_EARNINGS_GRADUATE,MEDIAN_EARNINGS_HS_GRAD,MEDIAN_EARNINGSLESS_THAN_HS,MEDIAN_FAMILY_INCOME,NUMBER_OF_RECORDS,POP_25_OVER,PROVIDER_CITY,PROVIDER_ID,PROVIDER_NAME,PROVIDER_STATE,PROVIDER_STREET_ADDRESS,PROVIDER_ZIP_CODE,filename,file_row_number,load_timestamp)
VALUES(s.id,s.AVERAGE_COVERED_CHARGES,s.AVERAGE_TOTAL_PAYMENTS,s.TOTAL_DISCHARGES,s.BACHELORORHIGHER,s.HSGRADORHIGHER,s.TOTALPAYMENTS,s.REIMBURSEMENT,s.TOTAL_COVERED_CHARGES,s.REFERRALREGION_PROVIDER_NAME,s.REIMBURSEMENTPERCENTAGE,s.DRG_DEFINITION,s.REFERRAL_REGION,s.INCOME_PER_CAPITA,s.MEDIAN_EARNINGSBACHELORS,s.MEDIAN_EARNINGS_GRADUATE,s.MEDIAN_EARNINGS_HS_GRAD,s.MEDIAN_EARNINGSLESS_THAN_HS,s.MEDIAN_FAMILY_INCOME,s.NUMBER_OF_RECORDS,s.POP_25_OVER,s.PROVIDER_CITY,s.PROVIDER_ID,s.PROVIDER_NAME,s.PROVIDER_STATE,s.PROVIDER_STREET_ADDRESS
       ,s.PROVIDER_ZIP_CODE,s.filename,s.file_row_number,s.load_timestamp);`})
return `DONE`;
$$;

CREATE OR REPLACE TASK ingest_data.public.healthcare_task 
WAREHOUSE='TRAINING_WH' 
SCHEDULE='1 minute'
WHEN SYSTEM$STREAM_HAS_DATA('ingest_data.public.healthcare_stream') AS
CALL ingest_data.public.upload_healthcare_data();

ALTER TASK ingest_data.public.healthcare_task RESUME;

DROP TASK ingest_data.public.healthcare_task;

SHOW TASKS;

select *
from table(information_schema.task_history(
    scheduled_time_range_start=>dateadd('hour',-4,current_timestamp()),
    result_limit => 6,
    task_name=>'healthcare_task'));
    
select * from ingest_data.public.HEALTHCARE;

delete from healthcare_raw
where id='5f2a4008590efd2fa89b2cd4';

select * from ingest_data.public.healthcare_raw;
select * from ingest_data.public.healthcare_stream;
select * from ingest_data.public.healthcare;
     

