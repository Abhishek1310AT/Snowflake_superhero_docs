USE INGEST_DATA;

CREATE OR REPLACE TABLE ingest_data.public.products(pname VARIANT);

create or replace storage integration s3_int_xml_pipe
  type = external_stage
  storage_provider = s3
  enabled = true
  storage_aws_role_arn = 'arn:aws:iam::696176995234:role/Snowflake-role'
  storage_allowed_locations = ('s3://snowflake-aws-demo1/snowflake/xml/');
  
DESC INTEGRATION s3_int_xml_pipe;

--create parquet format
create or replace file format INGEST_DATA.public.xml_format
  type = 'xml';

--Create external stage object
create or replace stage INGEST_DATA.public.ext_xml_stage_pipe
  URL = 's3://snowflake-aws-demo1/snowflake/xml/'
  STORAGE_INTEGRATION = s3_int_xml_pipe
  file_format = INGEST_DATA.public.xml_format;

create or replace pipe ingest_data.public.healthcare_pipe_xml auto_ingest=true as
COPY INTO ingest_data.public.products FROM @INGEST_DATA.public.ext_xml_stage_pipe FILE_FORMAT=(TYPE=XML) ON_ERROR='CONTINUE';


show pipes;

select * from ingest_data.public.products;

truncate table ingest_data.public.products;

alter pipe healthcare_pipe_xml refresh;


-- Validate

select SYSTEM$PIPE_STATUS('healthcare_pipe_xml')

select * from table(validate_pipe_load(
  pipe_name=>'ingest_data.public.healthcare_pipe_xml',
  start_time=>dateadd(hour, -4, current_timestamp())));



SELECT pname:"@issue"::STRING AS issue,
TO_DATE(pname:"@date"::STRING,'YYYY-MM-DD') AS date,
XMLGET(VALUE,'title'):"$"::STRING AS title,
COALESCE(XMLGET(VALUE,'genre'):"$"::STRING,
XMLGET(VALUE,'location'):"$"::STRING ) AS genre_or_location,
COALESCE(XMLGET( VALUE,'author'):"$"::STRING,
XMLGET(VALUE,'artist'):"$"::STRING) AS author_or_artist,
TO_DATE(XMLGET( VALUE,'publish_date'):"$"::String) AS publish_date,
XMLGET(VALUE,'price'):"$"::FLOAT AS price,
XMLGET(VALUE,'description'):"$"::STRING AS desc 
FROM ingest_data.public.products,
LATERAL FLATTEN(INPUT => pname:"$");

TRUNCATE TABLE ingest_data.public.products;