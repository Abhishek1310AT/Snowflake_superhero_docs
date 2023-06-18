// Practice #5

Select *
From Snowflake.Account_Usage.Warehouse_Load_History
Where Avg_Queued_Load > 0
Order By Start_Time Desc;
