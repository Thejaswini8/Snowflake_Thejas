-- This is your Cortex Project.
-----------------------------------------------------------
-- SETUP
-----------------------------------------------------------
use role ACCOUNTADMIN;
use warehouse SYS_WH;
use database BANKING;
use schema RAW;

-- Inspect the first 10 rows of your training data. This is the data we'll
-- use to create your model.
select * from TRANSACTIONS limit 10;

-- Inspect the first 10 rows of your prediction data. This is the data the model
-- will use to generate predictions.
select * from TRANSACTIONS limit 10;

-----------------------------------------------------------
-- CREATE PREDICTIONS
-----------------------------------------------------------
-- Create your model.
CREATE OR REPLACE SNOWFLAKE.ML.CLASSIFICATION my_model_thej(
    INPUT_DATA => SYSTEM$REFERENCE('TABLE', 'TRANSACTIONS'),
    TARGET_COLNAME => 'IS_FLAGGED',
    CONFIG_OBJECT => { 'ON_ERROR': 'SKIP' }
);

-- Inspect your logs to ensure training completed successfully. 
CALL my_model_thej!SHOW_TRAINING_LOGS();

-- Generate predictions as new columns in to your prediction table.
CREATE TABLE saving_predictions_try AS SELECT
    *, 
    my_model_thej!PREDICT(
        OBJECT_CONSTRUCT(*),
        -- This option alows the prediction process to complete even if individual rows must be skipped.
        {'ON_ERROR': 'SKIP'}
    ) as predictions
from TRANSACTIONS;

-- View your predictions.
SELECT * FROM saving_predictions_try;

-- Parse the prediction results into separate columns. 
-- Note: This is a just an example. Be sure to update this to reflect 
-- the classes in your dataset.
SELECT * EXCLUDE predictions,
        predictions:class AS class,
        round(predictions['probability'][class], 3) as probability
FROM saving_predictions_try;

-----------------------------------------------------------
-- INSPECT RESULTS
-----------------------------------------------------------

-- Inspect your model's evaluation metrics.
CALL my_model_thej!SHOW_EVALUATION_METRICS();
CALL my_model_thej!SHOW_GLOBAL_EVALUATION_METRICS();
CALL my_model_thej!SHOW_CONFUSION_MATRIX();

-- Inspect the relative importance of your features, including auto-generated features.  
CALL my_model_thej!SHOW_FEATURE_IMPORTANCE();
