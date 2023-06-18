------------------------------------------------------------------------------------------
--CREATING A WAREHOUSE with X-SMALL size for the demonstration
------------------------------------------------------------------------------------------
CREATE OR REPLACE WAREHOUSE MY_FIRST_WH 
WITH 
WAREHOUSE_SIZE = 'XSMALL' 
AUTO_SUSPEND = 300 
AUTO_RESUME = TRUE 
MIN_CLUSTER_COUNT = 1 
MAX_CLUSTER_COUNT = 5 
SCALING_POLICY = 'STANDARD' 
COMMENT = 'My First Warehouse';

--Setting the context
USE WAREHOUSE MY_FIRST_WH;


------------------------------------------------------------------------------------------
--Demonstrate  how increase in warehouse size helps in improving query execution time
------------------------------------------------------------------------------------------

---------------------------------------------------------------------------------------------------------------------
--Sample query to be executed on different warehouse sizes

USE DATABASE SNOWFLAKE_SAMPLE_DATA;
USE SCHEMA tpcds_sf10tcl; 
---------------------------------------------------------------------------------------------------------------------
--To ensure query doesnt uses result cache
ALTER SESSION SET USE_CACHED_RESULT = FALSE;

--Executing on X-SMALL Warehouse (1 minute 40 seconds)
with customer_total_return as
(select sr_customer_sk as ctr_customer_sk
,sr_store_sk as ctr_store_sk
,sum(SR_RETURN_AMT_INC_TAX) as ctr_total_return
from store_returns
,date_dim
where sr_returned_date_sk = d_date_sk
and d_year =1999
group by sr_customer_sk
,sr_store_sk)
 select  c_customer_id
from customer_total_return ctr1
,store
,customer
where ctr1.ctr_total_return > (select avg(ctr_total_return)*1.2
from customer_total_return ctr2
where ctr1.ctr_store_sk = ctr2.ctr_store_sk)
and s_store_sk = ctr1.ctr_store_sk
and s_state = 'NM'
and   ctr1.ctr_customer_sk = c_customer_sk
order by c_customer_id
limit 100 ;

--changing the warehouse size to SMALL
ALTER WAREHOUSE MY_FIRST_WH SET WAREHOUSE_SIZE = 'SMALL';

--To check if the state of warehouse, is resized or not
SHOW WAREHOUSES;

--Executing on SMALL Warehouse (47.81 seconds)
with customer_total_return as
(select sr_customer_sk as ctr_customer_sk
,sr_store_sk as ctr_store_sk
,sum(SR_RETURN_AMT_INC_TAX) as ctr_total_return
from store_returns
,date_dim
where sr_returned_date_sk = d_date_sk
and d_year =1999
group by sr_customer_sk
,sr_store_sk)
 select  c_customer_id
from customer_total_return ctr1
,store
,customer
where ctr1.ctr_total_return > (select avg(ctr_total_return)*1.2
from customer_total_return ctr2
where ctr1.ctr_store_sk = ctr2.ctr_store_sk)
and s_store_sk = ctr1.ctr_store_sk
and s_state = 'NM'
and   ctr1.ctr_customer_sk = c_customer_sk
order by c_customer_id
limit 100 ;

--changing the warehouse size to MEDIUM
ALTER WAREHOUSE MY_FIRST_WH SET WAREHOUSE_SIZE = 'MEDIUM';

--To check if the state of warehouse, is resized or not
SHOW WAREHOUSES;

--Executing on MEDIUM  Warehouse (23.41 seconds)
with customer_total_return as
(select sr_customer_sk as ctr_customer_sk
,sr_store_sk as ctr_store_sk
,sum(SR_RETURN_AMT_INC_TAX) as ctr_total_return
from store_returns
,date_dim
where sr_returned_date_sk = d_date_sk
and d_year =1999
group by sr_customer_sk
,sr_store_sk)
 select  c_customer_id
from customer_total_return ctr1
,store
,customer
where ctr1.ctr_total_return > (select avg(ctr_total_return)*1.2
from customer_total_return ctr2
where ctr1.ctr_store_sk = ctr2.ctr_store_sk)
and s_store_sk = ctr1.ctr_store_sk
and s_state = 'NM'
and   ctr1.ctr_customer_sk = c_customer_sk
order by c_customer_id
limit 100 ;

------------------------------------------------------
--DEMONSTRATING USE OF RESULT CACHE and SHOWING HOW QUERYING RESULT CACHE DOESNT NEED A WAREHOUSE
------------------------------------------------------

ALTER WAREHOUSE MY_FIRST_WH SUSPEND;

--Using the result cache (Show the query plan)
ALTER SESSION SET USE_CACHED_RESULT = TRUE;

with customer_total_return as
(select sr_customer_sk as ctr_customer_sk
,sr_store_sk as ctr_store_sk
,sum(SR_RETURN_AMT_INC_TAX) as ctr_total_return
from store_returns
,date_dim
where sr_returned_date_sk = d_date_sk
and d_year =1999
group by sr_customer_sk
,sr_store_sk)
 select  c_customer_id
from customer_total_return ctr1
,store
,customer
where ctr1.ctr_total_return > (select avg(ctr_total_return)*1.2
from customer_total_return ctr2
where ctr1.ctr_store_sk = ctr2.ctr_store_sk)
and s_store_sk = ctr1.ctr_store_sk
and s_state = 'NM'
and   ctr1.ctr_customer_sk = c_customer_sk
order by c_customer_id
limit 100 ;

--Changing the warehouse size back to X-SMALL
ALTER WAREHOUSE MY_FIRST_WH SET WAREHOUSE_SIZE = 'XSMALL';
SHOW WAREHOUSES;

---------------------------------------------------
---DEMONSTRATE THE USE OF WAREHOUSE CACHE
---------------------------------------------------

--Suspend the warehouse to delete the warehouse cache
ALTER WAREHOUSE MY_FIRST_WH SUSPEND;

--Run this query (1st time it will take more time & 2nd time it will return result quickly. Check query plan for both)
--In first case the IO would be 100% and Cache would be 0, and in second case cache would be 100% and IO would be zero.
select count(1) 
from web_returns 
where wr_order_number % 3 = 0 
and year(current_timestamp) > 0;

--Note the above query is not using result cache due to current_timestamp parameter
--Once you suspend the warehouse and rerun the query, then it will again perform full IO operation for first time.

--Change warehouse and see the performance of the query (It will be slower as its a different warehouse so warehouse cache is not available)
USE WAREHOUSE COMPUTE_WH;
select count(1) 
from web_returns 
where wr_order_number % 3 = 0 
and year(current_timestamp) > 0;

USE WAREHOUSE MY_FIRST_WH;
--Still will read from warehouse cache as its not suspended yet
select count(1) 
from web_returns 
where wr_order_number % 3 = 0 
and year(current_timestamp) > 0;

-----------------------------------------------------------------
--Demonstration of Metadata Cache use
-----------------------------------------------------------------

USE WAREHOUSE MY_FIRST_WH;
ALTER WAREHOUSE MY_FIRST_WH SUSPEND;
SELECT COUNT(*) FROM "SNOWFLAKE_SAMPLE_DATA"."TPCDS_SF10TCL"."CUSTOMER";
SELECT MIN(C_CUSTOMER_SK), MAX(C_CUSTOMER_SK) FROM "SNOWFLAKE_SAMPLE_DATA"."TPCDS_SF10TCL"."CUSTOMER";

---------------------------------------------------------------------
--DEMONSTRATE Usage of Cluster Warehouse (By Running many concurrent queries at one time)
---------------------------------------------------------------------

------------------------------------------------------------------------------------------
--CREATING A WAREHOUSE with X-SMALL size for the concurrent execution demonstration with Standard Scaling Policy
------------------------------------------------------------------------------------------
CREATE OR REPLACE WAREHOUSE MY_FIRST_CC_WH 
WITH 
WAREHOUSE_SIZE = 'XSMALL' 
AUTO_SUSPEND = 300 
AUTO_RESUME = TRUE 
MIN_CLUSTER_COUNT = 1 
MAX_CLUSTER_COUNT = 5 
SCALING_POLICY = 'STANDARD' 
COMMENT = 'My First Warehouse';


select WAREHOUSE_NAME, CLUSTER_NUMBER
, COUNT(*) NBR_OF_QUERIES
, SUM(TOTAL_ELAPSED_TIME/1000) ELAPSED_TIME_SEC
, SUM(COMPILATION_TIME) COMPILATION_TIME_MS
, SUM((QUEUED_PROVISIONING_TIME+QUEUED_REPAIR_TIME+QUEUED_OVERLOAD_TIME)/1000) QUEUE_TIME_SEC
from table(information_schema.query_history_by_warehouse('MY_FIRST_CC_WH'))
WHERE EXECUTION_STATUS = 'SUCCESS' and QUERY_TEXT LIKE 'select count(1)%'
GROUP BY WAREHOUSE_NAME, CLUSTER_NUMBER
;

------------------------------------------------------------------------------------------
--CREATING A WAREHOUSE with X-SMALL size for the concurrent execution demonstration with Economy Scaling Policy
------------------------------------------------------------------------------------------

CREATE OR REPLACE WAREHOUSE MY_FIRST_CC_ECO_WH 
WITH 
WAREHOUSE_SIZE = 'XSMALL'  
AUTO_SUSPEND = 300 
AUTO_RESUME = TRUE 
MIN_CLUSTER_COUNT = 1 
MAX_CLUSTER_COUNT = 5 
SCALING_POLICY = 'ECONOMY' 
COMMENT = 'My First Warehouse';

select WAREHOUSE_NAME, WAREHOUSE_TYPE, CLUSTER_NUMBER
, COUNT(*) NBR_OF_QUERIES
, SUM(TOTAL_ELAPSED_TIME/1000) ELAPSED_TIME_SEC
, SUM(COMPILATION_TIME) COMPILATION_TIME_MS
, SUM((QUEUED_PROVISIONING_TIME+QUEUED_REPAIR_TIME+QUEUED_OVERLOAD_TIME)/1000) QUEUE_TIME_SEC
from table(information_schema.query_history_by_warehouse('MY_FIRST_CC_ECO_WH'))
WHERE EXECUTION_STATUS = 'SUCCESS' and QUERY_TEXT LIKE 'select count(1)%'
GROUP BY WAREHOUSE_NAME, WAREHOUSE_TYPE,    CLUSTER_NUMBER
;
