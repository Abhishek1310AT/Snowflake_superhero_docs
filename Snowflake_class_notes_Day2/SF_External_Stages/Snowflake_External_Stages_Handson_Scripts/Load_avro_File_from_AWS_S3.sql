USE INGEST_DATA;

-- Create table to load AVRO data

------------- LOAD AVRO DATA -------------
CREATE or replace TABLE USER_AVRO(
    ID INT
   ,BIRTH_DATE VARCHAR(250)
   ,CC NUMBER  
   ,COMMENTS VARCHAR(250)
   ,COUNTRY VARCHAR(250)
   ,EMAIL VARCHAR(250)
   ,FIRST_NAME VARCHAR(250)
   ,GENDER VARCHAR(250)
   ,IP_ADDRESS VARCHAR(250)  
   ,LAST_NAME VARCHAR(250) 
   ,REGISTRATION_DTTM VARCHAR(250) 
   ,SALARY FLOAT   
   ,TITLE VARCHAR(250)  
   ,filename    VARCHAR(250)
   ,file_row_number VARCHAR(100)
   ,load_timestamp timestamp default TO_TIMESTAMP_NTZ(current_timestamp)
);

--CREATE OR REPLACE TABLE ingest_data.public.user_avro(uname VARIANT);

create or replace storage integration s3_int_avro
  type = external_stage
  storage_provider = s3
  enabled = true
  storage_aws_role_arn = 'arn:aws:iam::696176995234:role/Snowflake-role'
  storage_allowed_locations = ('s3://snowflake-aws-demo1/snowflake/avro/');
  
  desc integration s3_int_avro;
  
  create or replace file format INGEST_DATA.public.avro_format
  type = 'avro';
  
  --Create external stage object
create or replace stage INGEST_DATA.public.ext_avro_stage
  URL = 's3://snowflake-aws-demo1/snowflake/avro/'
  STORAGE_INTEGRATION = s3_int_avro
  file_format = INGEST_DATA.public.avro_format;
  
--COPY INTO ingest_data.public.user_avro FROM @INGEST_DATA.public.ext_avro_stage FILE_FORMAT=(TYPE=AVRO) ON_ERROR='CONTINUE';

select * from ingest_data.public.user_avro;

COPY INTO ingest_data.public.user_avro FROM 
(select 
$1:"id"::INT AS ID,
--TO_DATE($1:"birthdate"::STRING,'YYYY-MM-DD') AS BIRTH_DATE,
$1:"birthdate"::VARCHAR AS BIRTH_DATE,
$1:"cc"::INT AS CC,
$1:"comments"::VARCHAR AS COMMENTS,
$1:"country"::VARCHAR AS COUNTRY,
$1:"email"::VARCHAR AS EMAIL,
$1:"first_name"::VARCHAR AS FIRST_NAME,
$1:"gender"::VARCHAR AS GENDER,
$1:"ip_address"::VARCHAR AS IP_ADDRESS,
--TO_DATE($1:"registration_dttm"::STRING,'YYYY-MM-DD') AS REGISTRATION_DTTM,
$1:"registration_dttm"::VARCHAR AS REGISTRATION_DTTM,
$1:"last_name"::VARCHAR AS LAST_NAME,
$1:"salary"::FLOAT AS SALARY,
$1:"title"::VARCHAR AS TITLE,
METADATA$FILENAME,
METADATA$FILE_ROW_NUMBER,
TO_TIMESTAMP_NTZ(current_timestamp)
from @ingest_data.public.ext_avro_stage)
on_error = CONTINUE;

select * from ingest_data.public.user_avro;

TRUNCATE TABLE ingest_data.public.user_avro;