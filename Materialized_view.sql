


use database demo_db;

CREATE schema materializedViewDemo;

create or replace transient table store as
select * from 
SNOWFLAKE_SAMPLE_DATA.TPCDS_SF10TCL.STORE;

select * from STORE;

create or replace materialized view STORE_MV as
select * from STORE;

select * from STORE_MV;


-- it takes 1 hrs of time to get reflected, cannot reflect the recent changes
select * from TABLE(INFORMATION_SCHEMA.MATERIALIZED_VIEW_REFRESH_HISTORY());


-- it takes 3 hrs of time to get reflected
select * from SNOWFLAKE.ACCOUNT_USAGE.MATERIALIZED_VIEW_REFRESH_HISTORY;



SHOW VIEWS;

SHOW MATERIALIZED VIEWS LIKE 'STORE_MV';	-- observed "behind_by" columns

-- describe
desc MATERIALIZED VIEWS LIKE 'STORE_MV';

delete from STORE where S_Store_SK between 1 and 100;


-- rename a materialized view
alter materialized view STORE_MV rename to STORE_MVIEW;

-- add comments
alter materialized view STORE_MVIEW set comment = 'sample materialized view';


-- ********************************************************************

-- SECURE MATERIALIZED VIEW
create or replace SECURE materialized view STORE_SECURE_MV as 
select * from STORE;

select * from STORE_SECURE_MV;

SHOW VIEWS;

SHOW MATERIALIZED VIEWS LIKE 'STORE_SECURE_MV';

-- only the owner can see the definition of the materialized view under the "text" column.





