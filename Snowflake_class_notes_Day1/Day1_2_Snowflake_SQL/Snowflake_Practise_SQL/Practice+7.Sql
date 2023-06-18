// Practice #7

Select Sum(Bytes_Transferred) as Total_Bytes_Transferred
From Snowflake.Account_Usage.Data_Transfer_History
Where Source_Cloud <> Target_Cloud
