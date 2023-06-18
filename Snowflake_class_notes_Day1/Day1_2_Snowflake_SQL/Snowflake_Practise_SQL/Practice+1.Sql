// #1

Select 
    LH.User_Name,
    LH.Event_Timestamp as Login_Time,
    LH.Event_Id as Event_Id,
    Count(QH.Query_Id) as Query_Count,
    Avg(QH.Execution_Time) as Average_Query_Execution_Time
From Snowflake.Account_Usage.Login_History LH
    Join Snowflake.Account_Usage.Sessions S on LH.Event_Id = S.Login_Event_Id
    Join Snowflake.Account_Usage.Query_History QH on QH.Session_Id = S.Session_Id
Where LH.Event_Timestamp = :daterange    
Group By 1,2,3
Order By 4 Desc;