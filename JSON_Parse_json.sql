
-- PARSE_JSON vs TRY_PARSE_JSON vs CHECK_JSON



-- #### PARSE_JSON
-- accept valid json text (varchar) input
-- it could be an expression or field from table
-- return variant data
select parse_json( 'valid-string-text' ) ;


-- #### TRY_PARSE_JSON
-- accept valid json text (varchar) input
-- it could be an expression or field from table
-- return variant data
-- and if could not convert it to valid json, returns null
select try_parse_json( 'valid-string-text' ) ;



-- #### CHECK_JSON
-- accept valid json text (varchar) input
-- it could be an expression or field from table
-- return null if valid json or error msg if invalid json 
select check_json( 'valid-string-text' ) ;




-- use them together to get better results
-- use the case statement
select part_key,
case 
	when check_json(parts_txt) is null then 'valid'
	else 'not-valid'
end json_validity_flag,
case
	when check_json(parts_txt) is null then parse_json(parts_txt)
	else '{"JSON-Error":"'||check_json(parts_txt)||'"}'
end as json_data
from parts_json_as_txt
order by json_validity_flag;



