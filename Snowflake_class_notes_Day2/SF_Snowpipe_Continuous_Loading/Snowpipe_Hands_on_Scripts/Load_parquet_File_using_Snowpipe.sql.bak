use ingest_data;


SELECT * FROM ingest_data.PUBLIC.HEALTHCARE_PARQUET;

create or replace storage integration s3_int_parquet_pipe
  type = external_stage
  storage_provider = s3
  enabled = true
  storage_aws_role_arn = 'arn:aws:iam::696176995234:role/Snowflake-role'
  storage_allowed_locations = ('s3://snowflake-aws-demo1/snowflake/parquet/');
  
  DESC INTEGRATION s3_int_parquet_pipe;
  
  create or replace file format ingest_data.public.parquet_format
  type = 'parquet';
                    
create or replace stage ingest_data.public.ext_parquet_stage_pipe
  URL = 's3://snowflake-aws-demo1/snowflake/parquet/'
  --URL = 's3://snowflake-demo-test-26022022/csv/health_pipe.csv'
  STORAGE_INTEGRATION = s3_int_parquet_pipe
  file_format = ingest_data.public.parquet_format;


list @ingest_data.public.ext_parquet_stage_pipe;


create or replace pipe ingest_data.public.healthcare_pipe_parquet auto_ingest=true as
    -- Use copy command to ingest data from S3
copy into ingest_data.public.healthcare_parquet
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
from @ingest_data.public.ext_parquet_stage_pipe)
    on_error = CONTINUE;
    
show pipes;

select * from ingest_data.public.healthcare_parquet;

truncate table ingest_data.public.healthcare_parquet;

alter pipe healthcare_pipe_parquet refresh;


-- Validate

select SYSTEM$PIPE_STATUS('healthcare_pipe_parquet')

select * from table(validate_pipe_load(
  pipe_name=>'ingest_data.public.healthcare_pipe_parquet',
  start_time=>dateadd(hour, -4, current_timestamp())));
