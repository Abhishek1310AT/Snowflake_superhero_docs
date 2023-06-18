

#Step 1
# Download dependent jars--
# Spark Snowflake Connector
# Snowflake Driver

# Upload in s3 location--
# Create Glue Role
# create Glue Job -- and paste below code



#Case 1 (Read Complete Table) :-
from pyspark.sql import SparkSession
from pyspark import SparkContext

spark = SparkSession \
		.builder \
		.appName("Glue_job_Snowflake") \
		.getOrCreate()

def main():
	
	SNOWFLAKE_SOURCE_NAME = "net.snowflake.spark.snowflake"
	snowflake_database = ""
	snowflake_schema = ""
	source_table_name = ""
	snowflake_options = {
		"sfUrl": "xqa72164.snowflakecomputing.com",
		"sfUser": "",
		"sfPassword": "",
		"sfDatabase": snowflake_database,
		"sfSchema": snowflake_schema,
		"sfWarehouse": "COMPUTE_WH"
	}
	
	# Query data
	df = spark.read \
		.format(SNOWFLAKE_SOURCE_NAME) \
		.options(**snowflake_options) \
		.option("dbtable",snowflake_database+"."+snowflake_schema+"."+source_table_name) \
		.load()
	df1 = df.groupBy("department").sum("salary");
	
	
	# also use the SQL query shown below
	#   .option("query","select DEPARTMENT, sum(SALARY) as Salary from EMPLOYEE group by DEPARTMENT") \
	#	.load()
	#df.coalesce(1).write.option("mode","overwrite").option("header","true").csv("s3://mybucket/demo.csv")
	
	
	# save your resultset into snowflake
	df1.write.format("snowflake") \
		.options(**snowflake_options) \
		.option("dbtable", "destination_table").mode("overwrite") \
		.save()


# calling a function	
main()










#Case 2 (Reading data using SQL Query) :-
from pyspark.sql import SparkSession
from pyspark import SparkContext

spark = SparkSession \
		.builder \
		.appName("Glue_job_Snowflake") \
		.getOrCreate()

def main():
	
	SNOWFLAKE_SOURCE_NAME = "net.snowflake.spark.snowflake"
	snowflake_database = ""
	snowflake_schema = ""
	source_table_name = ""
	snowflake_options = {
		"sfUrl": "xqa72164.snowflakecomputing.com",
		"sfUser": "",
		"sfPassword": "",
		"sfDatabase": snowflake_database,
		"sfSchema": snowflake_schema,
		"sfWarehouse": "COMPUTE_WH"
	}
	
	# Query data
	df = spark.read \
		.format(SNOWFLAKE_SOURCE_NAME) \
		.options(**snowflake_options) \
	    .option("query","select DEPARTMENT, sum(SALARY) as Salary from EMPLOYEE group by DEPARTMENT") \
		.load()
	
	# save your resultset into snowflake
	df.coalesce(1).write.option("mode","overwrite").option("header","true").csv("s3://mybucket/demo.csv")



# calling a function	
main()




# Link
# https://www.youtube.com/watch?v=7c6kcRKDxgQ

