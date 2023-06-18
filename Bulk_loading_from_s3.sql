

-- CHANGE
-- CONNECT BY



***************************************************
**                FILE FORMAT                    **
***************************************************

-- CSV
CREATE OR REPLACE FILE FORMAT mycsvformat
   TYPE = 'CSV'
   FIELD_DELIMITER = '|'
   SKIP_HEADER = 1;
  

-- JSON
CREATE OR REPLACE FILE FORMAT myjsonformat
  TYPE = 'JSON'
  STRIP_OUTER_ARRAY = TRUE;

	-- STRIP_OUTER_ARRAY = TRUE 
	-- directs the COPY command to exclude the root brackets ([]) when loading data to the table.




***************************************************
**                    STAGE                      **
***************************************************

CREATE OR REPLACE STAGE external_stage
  FILE_FORMAT = mycsvformat
  URL = 's3://private-bucket'
  STORAGE_INTEGRATION = myint;



***************************************************
**                 COPY INTO                     **
***************************************************

COPY INTO mycsvtable
  FROM @my_csv_stage/tutorials/dataloading/contacts1.csv
  ON_ERROR = 'skip_file';
  
-- ON_ERROR clause, the default is abort_statement

COPY INTO mycsvtable
  FROM @my_csv_stage/tutorials/dataloading/
  PATTERN='.*contacts[1-5].csv'
  ON_ERROR = 'skip_file';




***************************************************
**              VALIDATE COPY INTO               **
***************************************************

CREATE OR REPLACE TABLE save_copy_errors AS SELECT * FROM TABLE(VALIDATE(mycsvtable, JOB_ID=>'<query_id>'));

SELECT * FROM SAVE_COPY_ERRORS;

-- Fix the Errors and Load the Data Files Again














