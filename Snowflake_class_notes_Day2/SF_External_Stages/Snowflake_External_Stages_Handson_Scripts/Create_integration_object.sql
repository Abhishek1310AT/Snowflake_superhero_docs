--- AWS S3 Configuration.

create or replace storage integration s3_int
  type = external_stage
  storage_provider = s3
  enabled = true
  storage_aws_role_arn = 'arn:aws:iam::***************:role/snowflake'
  storage_allowed_locations = ('s3://snowflake069/employee/');

DESC INTEGRATION s3_int;