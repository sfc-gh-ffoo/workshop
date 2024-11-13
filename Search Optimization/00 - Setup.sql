//Note: Use appropriate role having required privileges as mentioned 
//https://docs.snowflake.com/en/user-guide/search-optimization/enabling#required-access-control-privileges
USE ROLE accountadmin;

//Note: Substitute warehouse name my_wh with your warehouse name if different
USE WAREHOUSE my_wh;

//Note: Substitute database name so_analysis with your choice of database name(if required)
CREATE OR REPLACE DATABASE LLM_TRAINING_SO;

USE DATABASE LLM_TRAINING_SO;

SHOW SCHEMAS;

//Note: Create Schema name of your choice if you do not want to use PUBLIC schema
USE SCHEMA public;
CREATE SCHEMA CYBERSYN;
USE SCHEMA CYBERSYN;

//Note: Substitute my_wh with your warehouse name if different and use warehouse size of your choice
ALTER WAREHOUSE my_wh set warehouse_size='4x-large';

//This query time will depend on the warehouse size.
CREATE OR REPLACE TABLE OPENALEX_WORKS_INDEX AS SELECT * FROM LLM_TRAINING.CYBERSYN.OPENALEX_WORKS_INDEX; 

//Note: Check the table details
DESCRIBE TABLE OPENALEX_WORKS_INDEX;

SHOW TABLES LIKE 'OPENALEX_WORKS_INDEX' IN SCHEMA LLM_TRAINING_SO.CYBERSYN;

//Note: Check the table details by looking at the table DDL.
SELECT GET_DDL('table','LLM_TRAINING_SO.CYBERSYN.OPENALEX_WORKS_INDEX');

//Note: Check the data (Optional)
SELECT * FROM LLM_TRAINING_SO.CYBERSYN.OPENALEX_WORKS_INDEX LIMIT 100;
