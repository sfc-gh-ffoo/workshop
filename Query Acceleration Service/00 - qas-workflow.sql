/* 
QAS Workflow Tutorial: https://docs.snowflake.com/en/user-guide/tutorials/query-acceleration-service
*/

-- Step 1(a): Find an eligible query

SELECT query_id,
       query_text,
       start_time,
       end_time,
       warehouse_name,
       warehouse_size,
       eligible_query_acceleration_time,
       upper_limit_scale_factor,
       DATEDIFF(second, start_time, end_time) AS total_duration,
       eligible_query_acceleration_time / NULLIF(DATEDIFF(second, start_time, end_time), 0) AS eligible_time_ratio
FROM
    SNOWFLAKE.ACCOUNT_USAGE.QUERY_ACCELERATION_ELIGIBLE
WHERE
    start_time >= DATEADD(day, -7, CURRENT_TIMESTAMP())
    AND eligible_time_ratio <= 1.0
    AND total_duration BETWEEN 60 and 10 * 60
ORDER BY (eligible_time_ratio, upper_limit_scale_factor) DESC NULLS LAST
LIMIT 100;

-- Step 1(b): Find eligible warehouses by num eligible queries
SELECT warehouse_name, count(query_id) as num_eligible_queries, MAX(upper_limit_scale_factor)
  FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_ACCELERATION_ELIGIBLE
  WHERE start_time > DATEADD(month, -1, CURRENT_TIMESTAMP())
  GROUP BY warehouse_name
  ORDER BY num_eligible_queries DESC;

-- Step 1(c): Find eligible warehouses by most eligible time
SELECT warehouse_name, SUM(eligible_query_acceleration_time) AS total_eligible_time, MAX(upper_limit_scale_factor)
  FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_ACCELERATION_ELIGIBLE
  WHERE start_time > DATEADD(month, -1, CURRENT_TIMESTAMP())
  GROUP BY warehouse_name
  ORDER BY total_eligible_time DESC;


-- Step 2: Create Two New Warehouses to measure QAS effect
CREATE WAREHOUSE vinay_noqas WITH
  WAREHOUSE_SIZE='X-Small'
  ENABLE_QUERY_ACCELERATION = false
  INITIALLY_SUSPENDED = true
  AUTO_SUSPEND = 60;

CREATE WAREHOUSE vinay_qas WITH
  WAREHOUSE_SIZE='X-Small'
  ENABLE_QUERY_ACCELERATION = true
  QUERY_ACCELERATION_MAX_SCALE_FACTOR = 14
  INITIALLY_SUSPENDED = true
  AUTO_SUSPEND = 60;


-- Step 3: Query IDs with and without QAS
use schema SNOWFLAKE_SAMPLE_DATA.TPCDS_SF10TCL;
alter session set use_cached_result = false;

USE WAREHOUSE vinay_noqas;
USE WAREHOUSE vinay_qas;

/* TPC-DS: Query 7
Compute the average quantity, list price, discount, and sales price for promotional items 
sold in stores where the promotion is not offered by mail or a special event. 
Restrict the results to a specific gender, marital and educational status.
*/
select  i_item_id, 
        avg(ss_quantity) agg1,
        avg(ss_list_price) agg2,
        avg(ss_coupon_amt) agg3,
        avg(ss_sales_price) agg4 
 from store_sales, customer_demographics, date_dim, item, promotion
 where ss_sold_date_sk = d_date_sk and
       ss_item_sk = i_item_sk and
       ss_cdemo_sk = cd_demo_sk and
       ss_promo_sk = p_promo_sk and
       cd_gender = 'M' and 
       cd_marital_status = 'S' and
       cd_education_status = 'College' and
       (p_channel_email = 'N' or p_channel_event = 'N') and
       d_year = 2000 
 group by i_item_id
 order by i_item_id
 limit 100;

 SELECT LAST_QUERY_ID();
 -- vinay_noqas (2m46s): 01b85610-0806-c0c2-0016-cc03048a0b22
 -- vinay_qas (60s): 01b85621-0806-bcd3-0016-cc03048a264e

 
 -- Step 4: Compare query performance on the two warehouses
ALTER SESSION SET TIMEZONE = 'Asia/Singapore';
USE WAREHOUSE APP_WH;
SELECT query_id,
       query_text,
       warehouse_name,
       total_elapsed_time
FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
-- FROM TABLE(snowflake.information_schema.query_history())
WHERE query_id IN ('01b85610-0806-c0c2-0016-cc03048a0b22', '01b85621-0806-bcd3-0016-cc03048a264e')
ORDER BY start_time DESC;


-- Step 5: Compare cost on the two warehouses

-- NOQAS is WH Compute only
SELECT start_time,
       end_time,
       warehouse_name,
       credits_used,
       credits_used_compute,
       credits_used_cloud_services,
       (credits_used + credits_used_compute + credits_used_cloud_services) AS credits_used_total
FROM TABLE(SNOWFLAKE.INFORMATION_SCHEMA.WAREHOUSE_METERING_HISTORY(
    DATE_RANGE_START => DATEADD('days', -1, CURRENT_DATE()),
    WAREHOUSE_NAME => 'NOQAS_WH'
  ));

-- QAS has two cost components: WH Compute + QAS Serverless
SELECT start_time,
       end_time,
       warehouse_name,
       credits_used,
       credits_used_compute,
       credits_used_cloud_services,
       (credits_used + credits_used_compute + credits_used_cloud_services) AS credits_used_total
  FROM TABLE(SNOWFLAKE.INFORMATION_SCHEMA.WAREHOUSE_METERING_HISTORY(
    DATE_RANGE_START => DATEADD('days', -1, CURRENT_DATE()),
    WAREHOUSE_NAME => 'QAS_WH'
  ));

SELECT start_time,
         end_time,
         warehouse_name,
         credits_used
FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_ACCELERATION_HISTORY
WHERE START_TIME >= DATE_TRUNC('week', CURRENT_DATE)
    AND WAREHOUSE_NAME = 'QAS_WH'
;  
 

