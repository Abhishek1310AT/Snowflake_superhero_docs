

-- 

-- create db
create database analytics;
create warehouse transforming with warehouse_size='MEDIUM';

-- create role
create role transformer;


-- grant database access privileges to the role
grant IMPORTED PRIVILEGES on database snowflake_sample_data to role transformer;
grant usage on schema snowflake_sample_data.tpch_sf10 to role transformer;
grant select on all tables in schema snowflake_sample_data.tpch_sf10 to role transformer;


-- permission to modify database or create schema
grant usage on database analytics to role transformer;
grant reference_usage on database analytics to role transformer;
grant modify on database analytics to role transformer;
grant monitor on database analytics to role transformer;
grant create schema on database analytics to role transformer;


-- access to warehouse
grant operate on warehouse transforming to role transformer;
grant usage on warehouse transforming to role transformer;



-- create user
create user abhishektomar
email = 'abhiyou@gmail.com'
password = '12345'
default_role = transformer
default_warehouse = transforming
must_change_password = true;


-- grant role to user
grant role transformer to user abhishektomar;







