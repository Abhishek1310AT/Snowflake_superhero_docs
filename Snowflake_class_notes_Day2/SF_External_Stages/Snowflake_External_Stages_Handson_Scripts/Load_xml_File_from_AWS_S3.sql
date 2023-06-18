use ingest_data;

create or replace stage ingest_data.public.xml_stage;

CREATE OR REPLACE TABLE ingest_data.public.products(pname VARIANT);

--CREATE OR REPLACE TABLE ingest_data.public.productsxml(pname VARCHAR(150),date date,
--title VARCHAR(150),genre VARCHAR(150),location VARCHAR(150),author VARCHAR(150),artist VARCHAR(150),
--publish_date date,price float,description VARCHAR(150));

COPY INTO ingest_data.public.products FROM @~/xml/ FILE_FORMAT=(TYPE=XML) ON_ERROR='CONTINUE';

select * from ingest_data.public.products;

SELECT pname:"@issue"::STRING AS issue,
TO_DATE(pname:"@date"::STRING,'YYYY-MM-DD') AS date,
XMLGET(VALUE,'title'):"$"::STRING AS title,
COALESCE(XMLGET(VALUE,'genre'):"$"::STRING,
XMLGET(VALUE,'location'):"$"::STRING ) AS genre_or_location,
COALESCE(XMLGET( VALUE,'author'):"$"::STRING,
XMLGET(VALUE,'artist'):"$"::STRING) AS author_or_artist,
TO_DATE(XMLGET( VALUE,'publish_date'):"$"::String) AS publish_date,
XMLGET(VALUE,'price'):"$"::FLOAT AS price,
XMLGET(VALUE,'description'):"$"::STRING AS desc 
FROM ingest_data.public.products,
LATERAL FLATTEN(INPUT => pname:"$");

TRUNCATE TABLE ingest_data.public.products;