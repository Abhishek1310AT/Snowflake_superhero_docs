// Practice #3

Select 
   User_Name,
   Sum(Iff(Is_Success ilike 'No', 1, 0)) as Failed_Login_Count,
   Count(*) as Total_Logins,
   Sum(Iff(Is_Success ilike 'No', 1, 0)) / NullIf(Count(*), 0) as Login_Failure_Rate
From Snowflake.Account_Usage.Login_History
Where Event_Timestamp >= Date_Trunc(Month, Current_Date)
Group by User_Name
Order by 4 Desc;