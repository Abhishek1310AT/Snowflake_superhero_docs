

-- Metadata cache example
select count(*) FROM Date_dim;

-- No cache available, let's read from Remote Disk ( Remote in byte scanned )
select * from Date_dim;

-- Result cache example 2nd time again running same query. See result came from QUERY_RESULT_REUSE
select * from Date_dim;

-- Warehouse cache example ( local in Byte scanned )
alter session set use_cached_result = true;
select * from Date_dim;

-- The USE_CACHED_RESULT parameter disables to use of cached query results. 
-- It doesn't delete the existing caches. If you disable it, you can see the query plan (as you wanted), and 
-- your query will be executed each time without checking if the result is already available 



