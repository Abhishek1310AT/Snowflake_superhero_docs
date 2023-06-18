
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
df = session.read.parquet("@my_stage/case01/data_0_0.snappy.parquet")

df_schema = df.schema
print("\n\t Schema Object ", type(df_schema))
print("\n\t Numer of Columns: ", len(df_schema.fields),"\n")

for each_col in df_schema:
	print("\t\t", each_col.name, " => ", each_col.datatype)

print("\n\t Total Row Count in Dataframe: ", df.count())

print("\n\t First 10 records....")
df.show(10)

# save the data back to a table
df.write.save_as_table("cust_case01")

# close the session
session.close()





# case-2 (with header)
df = session.read.parquet("@my_stage/case02/data_0_0.snappy.parquet")

df_schema = df.schema
print("\n\t Schema Object ", type(df_schema))
print("\n\t Numer of Columns: ", len(df_schema.fields),"\n")

for each_col in df_schema:
	print("\t\t", each_col.name, " => ", each_col.datatype)

print("\n\t Total Row Count in Dataframe: ", df.count())

print("\n\t First 10 records....")
df.show(10)

# save the data back to a table
df.write.save_as_table("cust_case02")

# close the session
session.close()



# parquet file will automatically detect the appropriate header




# case-3 (multiple files + with header)
df = session.read.parquet("@my_stage/case03/")

df_schema = df.schema
print("\n\t Schema Object ", type(df_schema))
print("\n\t Numer of Columns: ", len(df_schema.fields),"\n")

for each_col in df_schema:
	print("\t\t", each_col.name, " => ", each_col.datatype)

print("\n\t Total Row Count in Dataframe: ", df.count())

print("\n\t First 10 records....")
df.show(10)

# save the data back to a table
df.write.save_as_table("cust_case03")

# close the session
session.close()





# case-4 (Transient table)
df = session.read.parquet("@my_stage/case03/")

df_schema = df.schema
print("\n\t Schema Object ", type(df_schema))
print("\n\t Numer of Columns: ", len(df_schema.fields),"\n")

for each_col in df_schema:
	print("\t\t", each_col.name, " => ", each_col.datatype)

print("\n\t Total Row Count in Dataframe: ", df.count())

print("\n\t First 10 records....")
df.show(10)

# save the data back to a table
df.write.save_as_table("cust_case04", table_type="transient")

# close the session
session.close()



# describe table
desc table cust_case04;

# get_dll
select get_dll('table','cust_case04')



