

-- Schema change
-- schemachange is a lightweight Python-based tool to manage all your Snowflake objects.
-- SCHEMACHANGE only support ".sql" file

-- gives the version name "V1.2__load_tables_from_s3.sql"



-- *****************************************************************

-- Initial setup
Drop database if exists SCHEMACHANGE_DEMO;
Drop database if exists METADATA;
Create or replace database METADATA;

---------------------------
Select * from METADATA.SCHEMACHANGE.CHANGE_HISTORY;
Select * from SCHEMACHANGE_DEMO.PUBLIC.TRIPS;
Select * from SCHEMACHANGE_DEMO.PUBLIC.WEATHER;


-- *****************************************************************
-- run below command in the command prompt

-- to see the help
> python cli.py -h
	
	-- it gives
		-f ROOT_FOLDER
		-c CHANGE_HISTORY_TABLE
		--vars VARS
		--create-chnage-history-table
		-ac --autocommit
		-v --verbose
		--dry-run Run Schemachange in dry run mode ( the default is False )
		
		


-- chnage database path ( it will create a METADATA.SCHEMACHANGE.CHANGE_HISTORY )
> python cli.py -f C:\Users\user\Downloads\citibike -a %SNOWFLAKE_ACCOUNT% -u %SNOWFLAKE_USER% -r %SNOWFLAKE_ROLE% -w %SNOWFLAKE_WAREHOUSE% -d %SNOWFLAKE_DATABASE% --create-change-history-table


-- run on SNOWFLAKE Web-UI
select * from METADATA.SCHEMACHANGE.CHANGE_HISTORY;


-- generate a detail log from CLI while deploying in Schemachange
> python cli.py -f C:\Users\user\Downloads\citibike -a %SNOWFLAKE_ACCOUNT% -u %SNOWFLAKE_USER% -r %SNOWFLAKE_ROLE% -w %SNOWFLAKE_WAREHOUSE% -d %SNOWFLAKE_DATABASE% -v


-- DML statement to auto commit from CLI
> python cli.py -f C:\Users\user\Downloads\citibike -a %SNOWFLAKE_ACCOUNT% -u %SNOWFLAKE_USER% -r %SNOWFLAKE_ROLE% -w %SNOWFLAKE_WAREHOUSE% -d %SNOWFLAKE_DATABASE% -ac


-- deploy with Dynamic variable defined in my script
> python cli.py -f C:\Users\user\Downloads\citibike -a %SNOWFLAKE_ACCOUNT% -u %SNOWFLAKE_USER% -r %SNOWFLAKE_ROLE% -w %SNOWFLAKE_WAREHOUSE% -d %SNOWFLAKE_DATABASE% --vars "{""DB"":""METADATA"", ""SCH"":""SCHEMACHANGE""}" -v -ac


-- dry run - to see the list of file going to deploy and in what sequence, rather than actual deployment
> python cli.py -f C:\Users\user\Downloads\citibike -a %SNOWFLAKE_ACCOUNT% -u %SNOWFLAKE_USER% -r %SNOWFLAKE_ROLE% -w %SNOWFLAKE_WAREHOUSE% -d %SNOWFLAKE_DATABASE% --dry-run

