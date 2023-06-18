USE INGEST_DATA;

-- Create table to load AVRO data

------------- LOAD AVRO DATA -------------
CREATE or replace TABLE USER_ORC(
FIRSTNAME VARCHAR(250)
,LASTNAME VARCHAR(250)
,EMAIL VARCHAR(250)
,GENDER VARCHAR(250)
,COUNTRY VARCHAR(250)
,filename VARCHAR(250)
,file_row_number VARCHAR(100)
,load_timestamp timestamp default TO_TIMESTAMP_NTZ(current_timestamp)
);

--CREATE OR REPLACE TABLE ingest_data.public.user_orc(uname VARIANT);

create or replace storage integration s3_int_orc_pipe
  type = external_stage
  storage_provider = s3
  enabled = true
  storage_aws_role_arn = 'arn:aws:iam::696176995234:role/Snowflake-role'
  storage_allowed_locations = ('s3://snowflake-aws-demo1/snowflake/orc/');
  
  desc integration s3_int_orc_pipe;
  
  create or replace file format INGEST_DATA.public.orc_format
  type = 'orc';
  
  --Create external stage object
create or replace stage INGEST_DATA.public.ext_orc_stage_pipe
  URL = 's3://snowflake-aws-demo1/snowflake/orc/'
  STORAGE_INTEGRATION = s3_int_orc_pipe
  file_format = INGEST_DATA.public.orc_format;
  
--COPY INTO ingest_data.public.user_avro FROM @INGEST_DATA.public.ext_avro_stage FILE_FORMAT=(TYPE=AVRO) ON_ERROR='CONTINUE';

select * from ingest_data.public.user_avro;

//LOOK AT DATA IN THE ORC FILE
SELECT t.$1 from @INGEST_DATA.public.ext_orc_stage_pipe (file_format => orc_format) t;

//QUERY TO GET DESIRED COLUMN NAMES
SELECT 
t.$1:"_col2"
,t.$1:"_col3"
,t.$1:"_col4"
,t.$1:"_col5"
,t.$1:"_col8"
,METADATA$FILENAME
,METADATA$FILE_ROW_NUMBER
,TO_TIMESTAMP_NTZ(current_timestamp)
from @INGEST_DATA.public.ext_orc_stage_pipe 
(file_format => orc_format) t;

--create or replace pipe ingest_data.public.user_pipe_orc auto_ingest=true as
COPY INTO ingest_data.public.user_orc FROM 
(SELECT 
t.$1:"_col2" AS FIRST_NAME
,t.$1:"_col3" AS LAST_NAME
,t.$1:"_col4" AS EMAIL
,t.$1:"_col5" AS GENDER
,t.$1:"_col8" AS COUNTRY
,METADATA$FILENAME
,METADATA$FILE_ROW_NUMBER
,TO_TIMESTAMP_NTZ(current_timestamp)
from @INGEST_DATA.public.ext_orc_stage_pipe 
(file_format => orc_format) t)
on_error = CONTINUE;

show pipes;

select * from ingest_data.public.user_orc;

/* truncate table ingest_data.public.user_orc;

alter pipe user_pipe_orc refresh;


-- Validate

select SYSTEM$PIPE_STATUS('user_pipe_orc')

select * from table(validate_pipe_load(
  pipe_name=>'ingest_data.public.user_pipe_orc',
  start_time=>dateadd(hour, -4, current_timestamp())));

select * from ingest_data.public.user_orc;

TRUNCATE TABLE ingest_data.public.user_orc; */