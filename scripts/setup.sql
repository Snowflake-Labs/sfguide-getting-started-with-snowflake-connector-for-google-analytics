USE ROLE accountadmin;

/*---------------------------*/
-- Create our Database
/*---------------------------*/
CREATE OR REPLACE DATABASE google_analytics;

/*---------------------------*/
-- Create our Schema
/*---------------------------*/
CREATE OR REPLACE SCHEMA google_analytics.raw_data;

/*---------------------------*/
-- Create our Warehouse
/*---------------------------*/

-- data science warehouse
CREATE OR REPLACE WAREHOUSE google_analytics_ds_wh
   WAREHOUSE_SIZE = 'xsmall'
   WAREHOUSE_TYPE = 'standard'
   AUTO_SUSPEND = 60
   AUTO_RESUME = TRUE
   INITIALLY_SUSPENDED = TRUE
   COMMENT = 'data science warehouse for google analytics';

-- Use our Warehouse
USE WAREHOUSE google_analytics_ds_wh;
/*---------------------------*/
-- sql completion note
/*---------------------------*/
SELECT 'google analytics sql is now complete' AS note;