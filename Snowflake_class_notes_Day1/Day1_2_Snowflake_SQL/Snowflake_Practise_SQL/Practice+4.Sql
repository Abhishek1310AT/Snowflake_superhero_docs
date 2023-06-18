// Practice #4

Select *
From Snowflake.Account_Usage.Columns
Where 
    Data_Type ilike 'Variant'
    And Deleted = :daterange;

