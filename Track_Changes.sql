

-- CHANGE
-- CONNECT BY



***************************************************
**                    CHANGE                     **
***************************************************


CREATE OR REPLACE TABLE t1 (
   id number(8) NOT NULL,
   c1 varchar(255) default NULL
 );

-- Enable change tracking on the table.
 ALTER TABLE t1 SET CHANGE_TRACKING = TRUE;

 -- Initialize a session variable for the current timestamp.
 SET ts1 = (SELECT CURRENT_TIMESTAMP());

 INSERT INTO t1 (id,c1)
 VALUES
 (1,'red'),
 (2,'blue'),
 (3,'green');

 DELETE FROM t1 WHERE id = 1;

 UPDATE t1 SET c1 = 'purple' WHERE id = 2;

 -- Query the change tracking metadata in the table during the interval from $ts1 to the current time.
 -- Return the full delta of the changes.
 SELECT *
 FROM t1
   CHANGES(INFORMATION => DEFAULT)
   AT(TIMESTAMP => $ts1);

 +----+--------+-----------------+-------------------+------------------------------------------+
 | ID | C1     | METADATA$ACTION | METADATA$ISUPDATE | METADATA$ROW_ID                          |
 |----+--------+-----------------+-------------------+------------------------------------------|
 |  2 | purple | INSERT          | False             | 1614e92e93f86af6348f15af01a85c4229b42907 |
 |  3 | green  | INSERT          | False             | 86df000054a4d1dc64d5d74a44c3131c4c046a1f |
 +----+--------+-----------------+-------------------+------------------------------------------+

 -- Query the change tracking metadata in the table during the interval from $ts1 to the current time.
 -- Return the append-only changes.
 SELECT *
 FROM t1
   CHANGES(INFORMATION => APPEND_ONLY)
   AT(TIMESTAMP => $ts1);

 +----+-------+-----------------+-------------------+------------------------------------------+
 | ID | C1    | METADATA$ACTION | METADATA$ISUPDATE | METADATA$ROW_ID                          |
 |----+-------+-----------------+-------------------+------------------------------------------|
 |  1 | red   | INSERT          | False             | 6a964a652fa82974f3f20b4f49685de54eeb4093 |
 |  2 | blue  | INSERT          | False             | 1614e92e93f86af6348f15af01a85c4229b42907 |
 |  3 | green | INSERT          | False             | 86df000054a4d1dc64d5d74a44c3131c4c046a1f |
 +----+-------+-----------------+-------------------+------------------------------------------+



--------------------------------------------------------------------------

-- Initialize a session 'end timestamp' variable for the current timestamp.
SET ts2 = (SELECT CURRENT_TIMESTAMP());

DELETE FROM t1;

-- Create a table populated by the change data between the start and end timestamps.
CREATE OR REPLACE TABLE t2 (
  c1 varchar(255) default NULL
  )
AS SELECT C1
  FROM t1
  CHANGES(INFORMATION => APPEND_ONLY)
  AT(TIMESTAMP => $ts1)
  END(TIMESTAMP => $ts2);

SELECT * FROM t2;

+-------+
| C1    |
|-------|
| red   |
| blue  |
| green |
+-------+






***************************************************
**                 CONNECT BY                    **
***************************************************

-- A CONNECT BY clause always joins a table to itself, not to another table.

-- The keyword PRIOR indicates that the value should be taken from the prior (higher/parent) level.
-- In this example, the current employee’s manager_ID should match the prior level’s employee_ID.

-- The keyword PRIOR should occur exactly once in each CONNECT BY expression. 
-- PRIOR can occur on either the left-hand side or the right-hand side of the expression, but not on both.


SELECT employee_ID, manager_ID, title
  FROM employees
    START WITH title = 'President'
    CONNECT BY
      manager_ID = PRIOR employee_id
  ORDER BY employee_ID;
+-------------+------------+----------------------------+
| EMPLOYEE_ID | MANAGER_ID | TITLE                      |
|-------------+------------+----------------------------|
|           1 |       NULL | President                  |
|          10 |          1 | Vice President Engineering |
|          20 |          1 | Vice President HR          |
|         100 |         10 | Programmer                 |
|         101 |         10 | QA Engineer                |
|         200 |         20 | Health Insurance Analyst   |
+-------------+------------+----------------------------+





SELECT SYS_CONNECT_BY_PATH(title, ' -> '), employee_ID, manager_ID, title
  FROM employees
    START WITH title = 'President'
    CONNECT BY
      manager_ID = PRIOR employee_id
  ORDER BY employee_ID;
+----------------------------------------------------------------+-------------+------------+----------------------------+
| SYS_CONNECT_BY_PATH(TITLE, ' -> ')                             | EMPLOYEE_ID | MANAGER_ID | TITLE                      |
|----------------------------------------------------------------+-------------+------------+----------------------------|
|  -> President                                                  |           1 |       NULL | President                  |
|  -> President -> Vice President Engineering                    |          10 |          1 | Vice President Engineering |
|  -> President -> Vice President HR                             |          20 |          1 | Vice President HR          |
|  -> President -> Vice President Engineering -> Programmer      |         100 |         10 | Programmer                 |
|  -> President -> Vice President Engineering -> QA Engineer     |         101 |         10 | QA Engineer                |
|  -> President -> Vice President HR -> Health Insurance Analyst |         200 |         20 | Health Insurance Analyst   |
+----------------------------------------------------------------+-------------+------------+----------------------------+



