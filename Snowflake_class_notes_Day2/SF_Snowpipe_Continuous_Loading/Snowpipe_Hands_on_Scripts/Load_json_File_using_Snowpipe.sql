use training_db;


SELECT * FROM TRAINING_DB.PUBLIC.HEALTHCARE_JSON;

create or replace storage integration s3_int
  type = external_stage
  storage_provider = s3
  enabled = true
  storage_aws_role_arn = 'arn:aws:iam::696176995234:role/Snowflake-role'
  storage_allowed_locations = ('s3://snowflake-aws-demo1/snowflake/json/');
  
  DESC INTEGRATION s3_int;
  
  create or replace file format training_db.public.json_format
  type = 'json';
                    
create or replace stage training_db.public.ext_json_stage
  URL = 's3://snowflake-demo-test-26022022/json/'
  --URL = 's3://snowflake-demo-test-26022022/csv/health_pipe.csv'
  STORAGE_INTEGRATION = s3_int
  file_format = training_db.public.json_format;


list @training_db.public.ext_json_stage


create or replace pipe training_db.public.healthcare_pipe_json auto_ingest=true as
    -- Use copy command to ingest data from S3
copy into training_db.public.healthcare_json
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
from @training_db.public.ext_json_stage)
    on_error = CONTINUE;
    
show pipes;

select * from training_db.public.healthcare_json;

truncate table training_db.public.emp_basic;

alter pipe healthcare_pipe_json refresh;


-- Validate

select SYSTEM$PIPE_STATUS('healthcare_pipe_json')

select * from table(validate_pipe_load(
  pipe_name=>'training_db.public.healthcare_pipe_json',
  start_time=>dateadd(hour, -4, current_timestamp())));
