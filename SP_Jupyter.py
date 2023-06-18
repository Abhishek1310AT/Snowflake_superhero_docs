
# connect Snowflake to Jupyter


# connect to Snowflake
session = Session.builder.configs(connection_parameters).create()
print(session.sql('select current_warehouse(), current_database(), current_schema()').collect())


# create dataframes for Snowflake tables
order_details_df = session.table('order_details')
order_details_df.limit(10).toPandas()

orders_df = session.table('orders')
orders_df.limit(10).toPandas()


# Enrich data with IPinfo Privacy dataset to determine if IP is masked 
privacy_df = session.table('ipinfo.public.privacy')
parse_ip = builtin("parse_ip")






