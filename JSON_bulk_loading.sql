

-- ## load Bulk-data

// load CSV file
create or replace table emp_basic (
	first_name string,
	last_name string,
	email string,
	streetaddress string,
	city string,
	start_date date
);


select * from emp_basic;





-- LOAD json file

create or replace STAGE my_json_stage file_format = (type = json);

-- put command to load the data into the stage

create or replace table relations_json_raw (
    json_data_raw VARIANT
);

-- load the json data
COPY INTO relations_json_raw FROM @json_data_raw;

-- display the json table
json_data_raw:Name,
    VALUE:Name::STRING,
    VALUE:Relationship::string
FROM
    relations_json_raw
        lateral flatten( input => json_data_raw:family_detail );

-- create a table
CREATE OR REPLACE TABLE candidate_family_detail AS 
SELECT 
    json_data_raw:Name,
    VALUE:Name::STRING,
    VALUE:Relationship::string
FROM
    relations_json_raw
        lateral flatten( input => json_data_raw:family_detail );

SELECT * FROM candidate_family_detail;






-- copy history table
select * form SNOWFLAKE.ACCOUNT_USAGE.COPY_HISTORY;
-- load history table
select * form SNOWFLAKE.ACCOUNT_USAGE.LOAD_HISTORY;



------------------------------------------------------------

-- JSON SAMPLE DATA
{
"device_type": "server",
"events": [
  {
    "f": 83,
    ..
  }
  {
    "f": 1000083,
    ..
  }
]}


SELECT src:device_type FROM raw_source;

+-----------------+
| SRC:DEVICE_TYPE |
|-----------------|
| "server"        |
+-----------------+



-- To retrieve these nested keys, you can use the FLATTEN function. The function flattens the events into separate rows.

SELECT
  value:f::number
  FROM
    raw_source
    LATERAL FLATTEN( INPUT => SRC:events );


+-----------------+
| VALUE:F::NUMBER |
|-----------------|
|              83 |
|         1000083 |
+-----------------+







