--Bulk Data Pipeline using AWS and External Stages

-- 1.GOALS
-- In this project, we will learn how to use snowflake as a query engine. We store our data 
-- in aws s3 and we will learn various methods to query it from snowflake.
-- A. Query data in s3 from snowflake.
-- B. Create view over data in aws s3.
-- C. Disadvantages and advantages of this approach.
create warehouse sys_wh
WAREHOUSE_SIZE='xsmall'
WAREHOUSE_TYPE='standard'
AUTO_SUSPEND=300
INITIALLY_SUSPENDED=TRUE;

Create database DEMO_DB;

/*
2. PREPARATION
Before we start, let’s upload some sample data from snowflake to s3. Then we will try to 
query data in s3 from snowflake.
Create table,
*/
CREATE OR REPLACE TRANSIENT TABLE DEMO_DB.PUBLIC.CUSTOMER_TEST
AS
SELECT * FROM 
"SNOWFLAKE_SAMPLE_DATA"."TPCDS_SF100TCL"."CUSTOMER";

ALTER SESSION SET USE_CACHED_RESULT = FALSE;

--creating stage

CREATE OR REPLACE STAGE DEMO_DB.PUBLIC.MY_S3_STAGE
URL='s3://snow3-07/snow-7/'
CREDENTIALS=(AWS_KEY_ID='XXXXXX' AWS_SECRET_KEY='XXXXXX');

--Execute below copy command to copy data to s3

COPY INTO @DEMO_DB.PUBLIC.MY_S3_STAGE/Customer_data/
from
DEMO_DB.PUBLIC.CUSTOMER_TEST;
/*
Query duration
1m 17s
Partitions scanned
216
Partitions total
216
Execution plan: TableScan (6.1%), Unload(took more time and processing here 93.5%), Result(0%)
*/


/*
3.QUERY DATA IN S3 FROM SNOWFLAKE.
Now data got uploaded to s3. We have 100 Million records uploaded and data size is 4.5 
GB. Uploaded files will be csv compressed files.
Let’s try to query this data in s3 from snowflake.
*/

--creating file format

CREATE OR REPLACE FILE FORMAT DEMO_DB.PUBLIC.MY_CSV_FORMAT
TYPE = 'CSV'
COMPRESSION = 'AUTO'
FIELD_OPTIONALLY_ENCLOSED_BY = '"'
SKIP_HEADER = 1;  
-- Querry duration 86ms

--querrying data from S3

SELECT $1 AS C_CUSTOMER_SK,
       $2 AS C_CUSTOMER_ID,
       $3 AS C_CURRENT_CDEMO_SK,
       $4 AS C_CURRENT_HDEMO_SK,
       $5 AS C_CURRENT_ADDR_SK,
       $6 AS C_FIRST_SHIPTO_DATE_SK,
       $7 AS C_FIRST_SALES_DATE_SK,
       $8 AS C_SALUTATION,
       $9 AS C_FIRST_NAME,
       $10 AS C_LAST_NAME,
       $11 AS C_PREFERRED_CUST_FLAG,
       $12 AS C_BIRTH_DAY,
       $13 AS C_BIRTH_MONTH,
       $14 AS C_BIRTH_YEAR,
       $16 AS C_LOGIN,
       $17 AS C_EMAIL_ADDRESS,
       $18 AS C_LAST_REVIEW_DATE
FROM @DEMO_DB.PUBLIC.MY_S3_STAGE/Customer_data/
(FILE_FORMAT => DEMO_DB.PUBLIC.MY_CSV_FORMAT);
/*
Query duration
1m 56s
*/

--Filter data directly from S3

SELECT $1 C_CUSTOMER_SK,
$2 C_CUSTOMER_ID ,
$3 C_CURRENT_CDEMO_SK ,
$4 C_CURRENT_HDEMO_SK ,
$5 C_CURRENT_ADDR_SK,
$6 C_FIRST_SHIPTO_DATE_SK ,
$7 C_FIRST_SALES_DATE_SK ,
$8 C_SALUTATION ,
$9 C_FIRST_NAME ,
$10 C_LAST_NAME,
$11 C_PREFERRED_CUST_FLAG ,
$12 C_BIRTH_DAY ,
$13 C_BIRTH_MONTH ,
$14 C_BIRTH_YEAR,
$16 C_LOGIN ,
$17 C_EMAIL_ADDRESS ,
$18 C_LAST_REVIEW_DATE
FROM @DEMO_DB.PUBLIC.MY_S3_STAGE/Customer_data/
(file_format => DEMO_DB.PUBLIC.MY_CSV_FORMAT)
WHERE C_CUSTOMER_SK ='64596949';
--querry duration 58s

--Execute group by,

SELECT $9 C_FIRST_NAME,$10 C_LAST_NAME,COUNT(*)
FROM @DEMO_DB.PUBLIC.MY_S3_STAGE/Customer_data/
(file_format => DEMO_DB.PUBLIC.MY_CSV_FORMAT)
GROUP BY $9,$10;
--Querry duration 57s
--External bytes scanned
--5.48GB
--Bytes written to result
--70.80MB
/*
4. CREATE VIEW OVER S3 DATA
*/

CREATE OR REPLACE VIEW CUSTOMER_DATA
AS
SELECT $1 C_CUSTOMER_SK,
$2 C_CUSTOMER_ID ,
$3 C_CURRENT_CDEMO_SK ,
$4 C_CURRENT_HDEMO_SK ,
$5 C_CURRENT_ADDR_SK,
$6 C_FIRST_SHIPTO_DATE_SK ,
$7 C_FIRST_SALES_DATE_SK ,
$8 C_SALUTATION ,
$9 C_FIRST_NAME ,
$10 C_LAST_NAME,
$11 C_PREFERRED_CUST_FLAG ,
$12 C_BIRTH_DAY ,
$13 C_BIRTH_MONTH ,
$14 C_BIRTH_YEAR,
$16 C_LOGIN ,
$17 C_EMAIL_ADDRESS ,
$18 C_LAST_REVIEW_DATE
FROM @DEMO_DB.PUBLIC.MY_S3_STAGE/Customer_data/
(file_format => DEMO_DB.PUBLIC.MY_CSV_FORMAT);
--querry duration 164ms

--Query data directly on view

SELECT * FROM CUSTOMER_DATA;
/*
Query duration
1m 55s
External bytes scanned
5.48GB
Bytes written to result
5.36GB
*/

/*
Now we can directly query data from s3 through view. What is the disadvantage of using 
this approach ? 
*/

/*
Disadvantages of This Approach

Performance Issues: Accessing data in S3 can be slower due to network latency and the additional overhead of accessing an external data source.
Inconsistent Performance: Performance may vary depending on network conditions and S3 service load.
Cost: Additional costs may arise from S3 data retrieval and Snowflake compute costs for processing external data.
Complexity: Managing file formats, schema evolution, and partitions can add complexity.
*/

/*
Now let’s try to Join the view we created with a table on snowflake,
Create a sample snowflake table as below,
*/
Create or replace transient table CUSTOMER_SNOWFLAKE_TABLE
AS
SELECT * FROM CUSTOMER_TEST limit 10000;
/*
Partitions scanned
2
Partitions total
215
*/

-- Join this with the view we created earlier

SELECT B.* 
FROM CUSTOMER_SNOWFLAKE_TABLE B
LEFT OUTER JOIN 
CUSTOMER_DATA A
ON
A.C_CUSTOMER_SK = B.C_CUSTOMER_SK;
/*
Partitions scanned
356
Partitions total
1
*/

/*
Now we successfully joined data in s3 with snowflake table. It may look simple but this 
approach has lot of potential. Can you mention few below,
page and observe the execution plan.
How many partitions got scanned from snowflake table 
*/

/*
5. UNLOAD DATA BACK TO S3
This approach leverages micro partitions in snowflake for lookup table still giving us the 
freedom to query data which we have stored in s3.
Once we are done looking up we can copy data back to s3 with new derived lookup column.
*/
COPY INTO @DEMO_DB.PUBLIC.MY_S3_STAGE/Customer_joined_data/
from(
SELECT B.* 
FROM CUSTOMER_SNOWFLAKE_TABLE B
LEFT OUTER JOIN 
CUSTOMER_DATA A
ON
A.C_CUSTOMER_SK = B.C_CUSTOMER_SK
);
/*
query duration: 1m 8s
Partitions scanned
355
Partitions total
1

Snowflake's internal storage is highly optimized for performance, including features like micro-partitioning and clustering. Querying external data in S3 does not benefit from these optimizations, potentially leading to slower query performance.

Latency: Querying data stored in S3 can introduce significant latency due to network overhead and the inherent delay in retrieving data from cloud storage.

Processing Time: Processing large volumes of data from S3 can be slower compared to querying data stored natively in Snowflake's optimized storage.

Data Transfer Costs: Frequent querying of data from S3 may incur additional costs related to data transfer and S3 GET requests.

Compute Costs: Snowflake charges for the compute resources used to process the queries, and queries involving external data may require more compute resources.
*/

/*
6.Advantages and Disadvantages

Advantages

Flexibility: Combines Snowflake's powerful query engine with the scalability and cost-effectiveness of S3 storage.
Cost-Effective: Saves costs by storing large volumes of data in S3 while using Snowflake for querying.
Scalability: Easily handles large datasets stored in S3 without the need to load all data into Snowflake.

Disadvantages

Performance Overhead: Queries involving S3 data may be slower due to network latency and additional processing.
Complexity in Data Management: Requires careful management of file formats, schema evolution, and partitioning in S3.
Additional Costs: Data transfer costs and increased compute costs for processing external data.
*/
