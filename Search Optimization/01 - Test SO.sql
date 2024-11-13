-- REFERENCE HOL: https://quickstarts.snowflake.com/guide/getting_started_with_search_optimization/index.html--


// Note: Optional to execute the queries before enabling Search Optimization on the table
ALTER SESSION SET use_cached_result = false; -- to clear cached data

ALTER WAREHOUSE my_wh SET warehouse_size = MEDIUM;

ALTER WAREHOUSE my_wh SUSPEND; -- to clear data cached at the warehouse level

ALTER WAREHOUSE my_wh RESUME;

ALTER WAREHOUSE my_wh SET warehouse_size= 'X-SMALL';

// Note: This query will take ~2 minutes 
SELECT *  from LLM_TRAINING_SO.CYBERSYN.OPENALEX_WORKS_INDEX where mag_work_id = 2240388798; 

// Note: This query will take ~2.5 minutes 
SELECT *  from LLM_TRAINING_SO.CYBERSYN.OPENALEX_WORKS_INDEX where work_title ilike 'Cross-domain applications of multimodal human-computer interfaces'; 

// Note: This query will take ~3 minutes 
SELECT * from LLM_TRAINING_SO.CYBERSYN.OPENALEX_WORKS_INDEX  where WORK_PRIMARY_LOCATION:source:display_name ilike 'Eco-forum'; 

// Note: This query will take ~4 minutes 
SELECT * from LLM_TRAINING_SO.CYBERSYN.OPENALEX_WORKS_INDEX  where WORK_PRIMARY_LOCATION:source:issn_l = '2615-6946'; 


//Note: Optional but recommended step

SELECT SYSTEM$ESTIMATE_SEARCH_OPTIMIZATION_COSTS('LLM_TRAINING_SO.CYBERSYN.OPENALEX_WORKS_INDEX',
                                               'EQUALITY(MAG_WORK_ID),EQUALITY(WORK_PRIMARY_LOCATION:source.display_name),
                                               SUBSTRING(WORK_TITLE),SUBSTRING(WORK_PRIMARY_LOCATION:source.issn_l)')
AS estimate_for_columns_without_search_optimization;


/******************* Defining Search Optimization on NUMBER fields For Equality *******************/
ALTER TABLE LLM_TRAINING_SO.CYBERSYN.OPENALEX_WORKS_INDEX ADD SEARCH OPTIMIZATION ON EQUALITY(MAG_WORK_ID);

// Defining Search Optimization on VARCHAR fields optimized for Wildcard search
ALTER TABLE LLM_TRAINING_SO.CYBERSYN.OPENALEX_WORKS_INDEX ADD SEARCH OPTIMIZATION ON SUBSTRING(WORK_TITLE);

// Defining Search Optimization on VARIANT field For Equality
ALTER TABLE LLM_TRAINING_SO.CYBERSYN.OPENALEX_WORKS_INDEX ADD SEARCH OPTIMIZATION ON SUBSTRING(WORK_PRIMARY_LOCATION:source.display_name); -- variant column

// Defining Search Optimization on VARIANT field For Wildcard search
ALTER TABLE LLM_TRAINING_SO.CYBERSYN.OPENALEX_WORKS_INDEX ADD SEARCH OPTIMIZATION ON EQUALITY(WORK_PRIMARY_LOCATION:source.issn_l);-- variant column

SHOW TABLES LIKE 'OPENALEX_WORKS_INDEX' IN SCHEMA LLM_TRAINING_SO.CYBERSYN

DESCRIBE SEARCH OPTIMIZATION ON LLM_TRAINING_SO.CYBERSYN.OPENALEX_WORKS_INDEX;


// Compare Results
-- Equality Search
SELECT *  
  FROM LLM_TRAINING_SO.CYBERSYN.OPENALEX_WORKS_INDEX 
  WHERE 
    mag_work_id = 2240388798;


-- Substring Search
SELECT *  
  FROM LLM_TRAINING_SO.CYBERSYN.OPENALEX_WORKS_INDEX 
  WHERE 
    work_title ilike 'Cross-domain applications of multimodal human-computer interfaces'; 

-- Search on Variant Column
select * 
  from LLM_TRAINING_SO.CYBERSYN.OPENALEX_WORKS_INDEX  
  where 
    WORK_PRIMARY_LOCATION:source:issn_l = '2615-6946';


select * 
  from LLM_TRAINING_SO.CYBERSYN.OPENALEX_WORKS_INDEX  
  where 
    WORK_PRIMARY_LOCATION:source:display_name ilike 'Eco-forum';


-- Example of a query that does not benefit from Search Optimization
select * 
  from LLM_TRAINING_SO.CYBERSYN.OPENALEX_WORKS_INDEX  
  where 
    WORK_PRIMARY_LOCATION:source:display_name ilike 'Reactions Weekly'; 