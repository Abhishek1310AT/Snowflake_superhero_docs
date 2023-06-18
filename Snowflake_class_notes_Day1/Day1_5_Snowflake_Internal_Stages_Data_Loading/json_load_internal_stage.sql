
---Loading the JSON Data using Internal Stage
CREATE OR REPLACE STAGE my_json_stage file_format = (type = json);

---Copy the JSON Data
PUT file://c:\temp\family.json @my_json_stage;


CREATE OR REPLACE TABLE relations_json_raw (
  json_data_raw VARIANT
);

---Load the JSON Data

COPY INTO relations_json_raw from @my_json_stage;

---Display the JSON Table
SELECT
    json_data_raw:Name, 
    VALUE:Name::String, 
    VALUE:Relationship::String 
FROM
    relations_json_raw
    , lateral flatten( input => json_data_raw:family_detail );
	
--Load Data into target table
--Now we have analyzed and extracted information. We can load the extracted data into the target table.

CREATE OR REPLACE TABLE candidate_family_detail AS
SELECT
    json_data_raw:Name AS candidate_name,
    VALUE:Name::String AS relation_name,
    VALUE:Relationship::String AS relationship

FROM
    relations_json_raw
    , lateral flatten( input => json_data_raw:family_detail );