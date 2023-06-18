-- What it can do.
use role securityadmin;

create or replace role myrole;

create or replace role myrole2;

grant role myrole to role myrole2;

drop role myrole;

-- What it can't do.

create database mydb;

create warehouse mywarehouse;


