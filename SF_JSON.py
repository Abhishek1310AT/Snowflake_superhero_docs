
# import snowflake snowpark library
from snowflake.snowpark import Session
from snowflake.snowpark import StructType, StructField, StringType
from snowflake.snowpark.functions import col

print("\n\tSnowpark Program Starting...")
# define connection parameter dictionary with role, database & schema
connection_parameters = {
	"ACCOUNT":"rfb4129",
	"USER":"demo05",
	"PASSWORD":"P@SSWORD",
	"ROLE":"SYSADMIN",
	"DATABASE":"DEMO",
	"SCHEMA":"snowpark"
}

# creating a session object
session = Session.builder.configs(connection_parameters).create()

# check if connection is through or not
print("\n\tFully Qualified Schema: ", session.get_fully_qualified_current_schema())




# case-1 (without header)
df = session.read.json("@my_stage/case01/data_0_0.json")

df_schema = df.schema
print("\n\t Schema Object ", type(df_schema))
print("\n\t Numer of Columns: ", len(df_schema.fields),"\n")

for each_col in df_schema:
	print("\t\t", each_col.name, " => ", each_col.datatype)

print("\n\t Total Row Count in Dataframe: ", df.count())

print("\n\t First 2 records....")
df.show(2)

# save the data in a TEMPORARY TABLE as a VARIENT columns name "$1" 
df.write.save_as_table("customer_tmp", table_type="temporary")

df_new = session.sql("select $1:SALUTATION::TEXT as SALUTATION, \
					$1:FIRST_NAME::TEXT as FIRST_NAME, \
					$1:LAST_NAME::TEXT as LAST_NAME, \
					$1:BIRTH_DATE::DATE as BIRTH_DATE, \
					$1:BIRTH_COUNTRY::TEXT as BIRTH_COUNTRY, \
					$1:EMAIL_ADDRESS::TEXT as EMAIL_ADDRESS, \
					FROM CUSTOMER_TMP")


# save the data back to the table
df_new.write.save_as_table("customer_json03")

# close the session
session.close()

print("\n\tSnowpark Program Ended...")




# describe table
desc table customer_json03;

# get_dll
select get_dll('table','customer_json03')


