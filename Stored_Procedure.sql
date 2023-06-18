

-- Create an stored procedure

CREATE OR REPLACE PROCEDURE myprocedure()
  RETURNS VARCHAR
  LANGUAGE SQL
  AS
  $$
    -- Snowflake Scripting code
    -- DECLARE variable
	DECLARE
      radius_of_circle FLOAT;
      area_of_circle FLOAT;
    BEGIN
      radius_of_circle := 3;
      area_of_circle := pi() * radius_of_circle * radius_of_circle;
      RETURN area_of_circle;
    END;
  $$
  ; 




  
-- If you are writing an anonymous block, 
-- pass the block as a string literal to the EXECUTE IMMEDIATE command.
 
EXECUTE IMMEDIATE 
$$
-- Snowflake Scripting code
DECLARE
  radius_of_circle FLOAT;
  area_of_circle FLOAT;
BEGIN
  radius_of_circle := 3;
  area_of_circle := pi() * radius_of_circle * radius_of_circle;
  RETURN area_of_circle;
END;
$$
;




-- As an alternative, you can define a session variable that is a string literal containing the block, 
-- and you can pass that session variable to the EXECUTE IMMEDIATE command.

set stmt =
$$
declare
    radius_of_circle float;
    area_of_circle float;
begin
    radius_of_circle := 3;
    area_of_circle := pi() * radius_of_circle * radius_of_circle;
    return area_of_circle;
end;
$$
;

execute immediate $stmt;







-- Sample SP
CREATE OR REPLACE PROCEDURE output_message(message VARCHAR)
RETURNS VARCHAR not NULL
LANGUAGE SQL
AS
$$
BEGIN
	return message;
END;
$$
;

call output_message('Hello World');


-- alter (without argument)
CREATE OR REPLACE PROCEDURE output_message()
RETURNS VARCHAR not NULL
LANGUAGE SQL
AS
$$
DECLARE VAR1 VARCHAR DEFAULT 'Hello World';
BEGIN
	return VAR1;
END;
$$
;

call output_message();


-- alter (assign a variable)
CREATE OR REPLACE PROCEDURE output_message()
RETURNS VARCHAR not NULL
LANGUAGE SQL
AS
$$

BEGIN
	let VAR1 VARCHAR:='Hello World';
	return VAR1;
END;
$$
;

call output_message();


-- Assign your variable in the SQL statement
CREATE OR REPLACE PROCEDURE output_message()
RETURNS VARCHAR not NULL
LANGUAGE SQL
AS
$$

BEGIN
	let VAR1 VARCHAR:='Hello World';
	CREATE OR REPLACE TABLE TEST AS select :VAR1 as Col2;
END;
$$
;

call output_message();

select * from 'PUBLIC'.'TEST';



-- Select a value INTO a variable
CREATE OR REPLACE PROCEDURE output_message()
RETURNS VARCHAR not NULL
LANGUAGE SQL
AS
$$

BEGIN
	let VAR1 INT;
	SELECT 10 INTO :VAR1;
	RETURN VAR1;
END;
$$
;

call output_message();

select * from 'PUBLIC'.'TEST';




-- If statement
EXECUTE IMMEDIATE
$$
DECLARE VAR1 DEFAULT 9;
begin 
	IF (VAR1 > 10) THEN
	RETURN 'more than 10';
	ELSE
	RETURN 'less than 10';
	END IF;
end;
$$;





