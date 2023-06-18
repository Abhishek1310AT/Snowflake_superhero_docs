
# import snowflake—snowpark library
from snowflake.snowpark import Session
from snowflake.snowpark.types import StructType, StructField, StringType
from snowflake.snovpark.functions import col, year
from datetime import datetime


# add these two imports...
import sys
import logging

logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)

print("\n################### ^^ Logging Operation\n")

# to store all logging into a file
# logging.basicConfig(filename='./snowpark.log', level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s', datefmt='%I:%M:%S')


# define connection parameter dictionary with role *database schema
connection_paraneters = {
	"ACCOUNT" : "abb4Ø127" ,
	"USER" : "demo",
	"PASSWORD" : "Passw0rd#",
	"ROLE": "SYSADMIN" ,
	"DATABASE": "demo" ,
	"SCHEMA" : "snowpark"
}

# creating snowflake session object
session = Session.builder.configs(connection_paraneters).create()

# customer table has 10,000
customer_df = session.sql("select * from customer")

# applying 2 filters
customer_df = customer_df.filter(col("SALUTATION")=='Dr.').filter(col("BIRTH_COUNTRY")=="FINLAND")

# adding a new column year
customer_df = customer_df.withColumn("YEAR", year("DATE_OF_BIRTH"))

# running a group by statement
customer_df = customer_df.group_by(col("YEAR")).count()

print("\n################### ^^ DF Operations\n")


# show first 3 customer data
customer_df.show(3)
print("\n################### ^^ show()\n")

# calling the explain function
customer_df.explain()
print("\n################### ^^ Explain()\n")

# close the session
session.close()
print("\n################### ^^ Close()\n")







