----------------- CREATE -----------------
-- create the database or its objects (warehouse, database, schema, table, stage, file format, role, user,
--                                     function, views, triggers and store procedure).

-- create virtual warehouse
create warehouse test;           -- By default it creates XS-Extra Small size warehouse

-- Warehouse sizes are similar to T-SHIRT sizes from XS to 4X-LARGE
-- Provide warehouse NAME as it is mandatory 
-- Below are optional parameters that CREATE WAREHOUSE statement
/*objectProperties ::=
  WAREHOUSE_SIZE = XSMALL | SMALL | MEDIUM | LARGE | XLARGE | XXLARGE | XXXLARGE | X4LARGE
  MAX_CLUSTER_COUNT = <num>
  MIN_CLUSTER_COUNT = <num>
  SCALING_POLICY = STANDARD | ECONOMY
  AUTO_SUSPEND = <num> | NULL
  AUTO_RESUME = TRUE | FALSE
  INITIALLY_SUSPENDED = TRUE | FALSE
  RESOURCE_MONITOR = <monitor_name>
  COMMENT = '<string_literal>'*/


-- SCALING_POLICY = 
	-- STANDARD : check 5-6 times query in every interval of 1min, check the load in every 20sec 
	-- ECONOMY : check 2-3 times query in every interval of 1min, check the load in every 6min 


create warehouse my_wh WAREHOUSE_SIZE = MEDIUM;

-- To suspend a warehouse
alter warehouse test suspend;

-- if warehouse is in suspended state, we can resume it by running below statement
alter warehouse test resume;

-- list warehouses
show warehouses;

use warehouse compute_wh;
use warehouse test;

-- To know more about the usage of above additional parameters, refer to below URL:
-- https://docs.snowflake.com/en/sql-reference/sql/create-warehouse.html