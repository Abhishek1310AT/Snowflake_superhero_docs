



-- create a 'customer_validation' table
create or replace transient table customer_validation (
	customer_pk number(38,0),
	first_name varchar(55),
	gender varchar(1),
	dob date,
	country varchar(20)
);


-- create FILE FORMAT
create or replace file format csv_ff
	type = 'csv'
	compression = 'none'
	field_delimiter = ','
	record_delimiter = '\n'
	skip_header = 1
	field_optionally_encoded_by = '\047';


-- lets load the data using put command
	put file:///tmp/ch08/small-csv/*.csv
		@~ch08/small-csv
		auto_compress = false;
*/

list @~ch08/small-csv



-- load data from stage to table
copy into customer_validation
	from @~/ch08/small-csv/customer_01.csv
	file_format = csv_ff
	on_error = 'continue'
	force = true
	validation_mode = return_errors;


-- option-1
	-- validation_mode = return_errors;
	-- Return all errors (parsing, conversion, others) across all files specified into the COPY statement
	
-- option-2
	-- validation_mode = return_1_rows;
	-- Return all errors (parsing, conversion, others) across all files specified into the COPY statement

-- option-3
	-- validation_mode = return_all_errors;
	-- Return all errors (parsing, conversion, others) across all files specified into the COPY statement




-- load data from stage to table (BULK LOADING) with pattern match
copy into customer_validation
	from @~/ch08/csv/partition
	file_format = csv_gz_ff
	on_error = 'continue'
	force = true
	pattern = '.*[.]csv[.]gz'
	validation_mode = return_all_errors;



-- 'validate' is a table function
select * from table (validate(customer_validation, job_id => 'query_id'));

