-- connect to snowflake

> Help > Download > snowsql

cmd (run as administrator)
> snowsql -a ggphfzy-tu49803
User: abhishek1310
Pass: Career@2022

OR

-- by giving the credentials in config file
cmd (run as administrator)
> snowsql --config C:\Users\user\.snowsql\config
Password:  



-- create table on snowflake:
create or replace table emp_basic (
	first_name string,
	last_name string,
	email string,
	streetaddress string,
	city string,
	start_date date
	);


> use db;

-- PUT command to upload the data
> put file://C:\Users\user\Desktop\CSV_DATA\employees0*.csv @test.PUBLIC.%EMP_BASIC;

> list @%emp_basic;

-- Copy data from the staged files into an existing target table. 
-- A running warehouse is required for this step.
> copy into emp_basic
	from @%emp_basic
	file_format = (type = csv field_optionally_enclosed_by='"')
	pattern = '.*employees0[1-5].csv.gz'
	on_error = 'skip_file';


-- remove the particular file from the staging via command-line, before COPY into Snowflake
> remove @~/employees01.csv.gz;
> REMOVE @my_csv_stage PATTERN='.*.csv.gz';

-- To exit a connection, use the !exit command for SnowSQL (or its alias, !disconnect).


-- The COPY command supports:
	-- Column reordering, column omission, and casts using a SELECT statement. There is no requirement for your data files to have the same number and ordering of columns as your target table.
	-- The ENFORCE_LENGTH | TRUNCATECOLUMNS option, which can truncate text strings that exceed the target column length.





---------------------------------------------------

-- consolidated on Snowflake


-- Create integration object for external stage
create or replace storage integration s3_int_csv
	type = external_stage
	storage_provider = s3
	enabled = true
	storage_aws_role_arn = 'arn:aws:iam::074845479790:role/Snowflake-role1'
	storage_allowed_locations = ('s3://snowflake-data-08012023/csv/');


-- Describe integration object to fetch "external_id" and to be used in s3
DESC INTEGRATION s3_int_csv ;

-- Create file format csv_pipe_file
create or replace file format ingest_data.public.csv_format
	type = csv
	field_delimiter = '|'
	skip_header = 1
	null_if = ('NULL', 'null')
	empty_field_as_null = True ;


-- Create an external stage object
create or replace stage ingest_data.public.ext_csv_stage
	--URL = 's3:// snowflake-aws-demol/snowflake/csv/health.csv'
	URL = 's3://snowflake-aws-demol/snowflake/csv/'
	STORAGE_INTEGRATION = s3_int_csv
	file_format = ingest_data.public.csv_format;

LIST @ingest_data.public.ext_csv_stage ;

-- Use copy command to ingest data from s3
copy into healthcare_csv
from ingest_data.public.ext_csv_stage
on_error = CONTINUE ;


SELECT * from healthcare_csv;



---------------------------------------------------



# json file


-- Create integration object for external stage
create or replace storage integration s3_int_json
	type = external_stage
	storage_provider = s3
	enabled = true
	storage_aws_role_arn = 'arn:aws:iam::074845479790:role/Snowflake-role1'
	storage_allowed_locations = ('s3://snowflake-data-08012023/json/');


-- Describe integration object to fetch external_id and to be used in s3
DESC INTEGRATION s3_int_json ;

-- Create file format csv_pipe_file
create or replace file format ingest_data.public.json_format
	type = 'json' ;


-- Create an external stage object
create or replace stage ingest_data.public.ext_json_stage
	URL = 's3://snowflake-aws-demol/snowflake/json/'
	STORAGE_INTEGRATION = s3_int_json
	file_format = ingest_data.public.json_format;

LIST @ingest_data.public.ext_json_stage ;

-- Use copy command to ingest data from s3
copy into healthcare_json
from ingest_data.public.ext_json_stage
on_error = CONTINUE ;

?????????????



---------------------------------------------------


-- creating pipe line

CREATE OR REPLACE PIPE training_db.public.healthcare_pipe_json auto_ingest=True AS
copy into ingest_data.public.healthcare_json
from (select

) ;


alter pipe healthcare_pipe_json REFRESH ;

-- Validate
select SYSTEM$PIPE_STATUS('healthcare_pipe_json')





---------------------------------------------------

-- Create a stream object 
create or replace stream sales_stream on table sales_raw_staging ;

SHOW STREAMS ;

DROP STREAM sales_stream ;

-- Get changes on data using stream (INSERT)
select * from sales_stream ; 
select * from sales_raw_staging ; 



merge into SALES_FINAL_TABLE F		-- Target table to merge changes from source table
using SALES_STREAM S				-- Stream that has captured the changes
	on f.id = S.id
when matched
	and S.METADATA$ACTION = 'DELETE'
	and S.METADATA$ISUPDATE = 'FALSE'
	then delete
	


---------------------------------------------------

CREATE TABLE EMPLOYEES(EMPLOYEE_ID INTEGER AUTOINCREMENT START = 1 INCREMENT = 1,
	EMPLOYEE_NAME VARCHAR DEFAULT 'SREE' ,
	LOAD_TIME DATE);

CREATE OR REPLACE TASK EMPLOYEES_TASK
	WAREHOUSE = COMPUTE_WH
	SCHEDULE = '1 MINUTE'
AS
	INSERT INTO EMPLOYEES(LOAD_TIME) VALUES(CURRENT_TIMESTAMP);


SHOW TASKS ;




---------------------------------------------------

create table trips_dev clone trips



grant select on future tables in schema d1.s1 to role read_only;
grant insert,delete on future tables in schema d1.s1 to role r2;
show grants to role r1;
grant role accountadmin, sysadmin to user user2;
alter user user2 set email='user2@domain.com', default_role=sysadmin;






CREATE ROLE "IMPORT_ROLE";
GRANT ROLE "IMPORT_ROLE" TO ROLE "PUBLIC";

---------------------------------------------------

// Snowflake.Account_Usage

Select *
From Snowflake.Account_Usage.Warehouse_Load_History
Order By Start_Time Desc;


Select *
From Snowflake.Account_Usage.Data_Transfer_History;


Select *
From Snowflake.Account_Usage.Login_History;


Select *
From Snowflake.Account_Usage.Columns
Where 
    Data_Type ilike 'Variant';
//    And Deleted = :daterange;

---------------------------------------------------






























