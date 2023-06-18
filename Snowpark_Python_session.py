


from snowflake.snowpark import Session

# connection parameter
# just account name and user/pwd
connection_param = {
    "ACCOUNT":"ABC12345",
    "USER":"<my-user-id>",
    "PASSWORD":"<my-password>"
}
# print connection params
print("The Parameter :",connection_param)

# creating a session object
session = Session.builder.configs(connection_param).create()

# print values from session object to test
print("\n\t Current Account Name: ",session.get_current_account())
print("\t Current Database Name: ",session.get_current_database())
print("\t Current Schema Name: ",session.get_current_schema())
print("\t Current Role Name: ",session.get_current_role())
print("\t Current Warehouse Name: ",session.get_current_warehouse())
print("\t Fully Qualified Schema Name: ",session.get_fully_qualified_current_schema(),"\n")

print("Session Object Type:", type(session))

# closing the session
session.close()



######################################################################


import snowflake.snowpark.functions as f
from snowflake.snowpark import Session, DataFrame
from snowflake.snowpark.functions import udf, col
from snowflake.snowpark.types import IntergerType
from snowflake.snowpark.functions import call_udf

Warehouse_Name = 'MY_DEMO_WH'
Warehouse_Size = "LARGE"
DB_Name = 'DEMO_SNOWPARK'
Schema_Name = 'Public'


CONNECTIONS_PARAMETERS= {
	'account': '<Snowflake_Account>',
	'user': 'Someuser',
	'password': '12345',
	'role': 'SYSADMIN'
}


session = Session.builder.configs(CONNECTIONS_PARAMETERS).create()

session.use_database(DB_Name)
session.use_schema(Schema_Name)
session.use_warehouse(Warehouse_Name)


# load data

dfLineItems = session.table("SNOWFLAKE_SAMPLE_DATA.TPCH_SF100.LINEITEM")















