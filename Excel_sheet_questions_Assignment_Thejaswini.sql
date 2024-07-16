/*
1. How will you use to change the warehouse for workload processing to a warehouse named ‘COMPUTE_WH_XL’?
*/
USE WAREHOUSE COMPUTE_WH_XL;

/*
2. Consider a table vehicle_inventory that stores vehicle information of all vehicles in your dealership. The table has only one VARIANT column called vehicle_data which stores information in JSON format. The data is given below:
{
“date_of_arrival”: “2021-04-28”,
“supplier_name”: “Hillside Honda”,
“contact_person”: {
“name”: “Derek Larssen”,
“phone”: “8423459854”
},
“vehicle”: [
{
“make”: “Honda”,
“model”: “Civic”,
“variant”: “GLX”,
“year”: “2020”
}
]
}
What is the command to retrieve supplier_name?
*/

-- Create the vehicle_inventory table
CREATE OR REPLACE TABLE vehicle_inventory (
    vehicle_data VARIANT
);

INSERT INTO vehicle_inventory (vehicle_data)
SELECT PARSE_JSON('{
    "date_of_arrival": "2021-04-28",
    "supplier_name": "Hillside Honda",
    "contact_person": {
        "name": "Derek Larssen",
        "phone": "8423459854"
    },
    "vehicle": [
        {
            "make": "Honda",
            "model": "Civic",
            "variant": "GLX",
            "year": "2020"
        }
    ]
}');

-- Query the supplier_name from the vehicle_data column
SELECT vehicle_data:supplier_name AS supplier_name
FROM vehicle_inventory;

/*
3. From a terminal window, how to start SnowSQL from the command prompt ? And write the steps to load the data from local folder into a Snowflake table usin three types of internal stages.
*/

Open command prompt
SNOWSQL -a qjhtlxv-msthejaswinir
User: msthejaswinir
Password: xyz

>use role accountadmin
>use warehouse compute_wh
>use database injest

first need to create target table structure

>create or replace table emp_basic
(first_name string, last_name string, email string, streetaddress string, city string, start_date date);

use PUT command to copy data from local to stage and COPY INTO command for copying data from stage to table

using user stage:
>list @~;
>PUT file://c:\temp\employee*.csv @~;
>copy into emp_basic
 from @~
 file_format=(type=csv_field_optionally_enclosed_by='')
 pattern='.*employeeo[1-5].csv.gz'
 on_error='SKIP_FILE'
>select * from emp_basic;

using table stage:
>list @%emp_basic;
>PUT file://c:\temp\employee*.csv @%emp_basic;
>copy into emp_basic
 from %emp_basic
 file_format=(type=csv_field_optionally_enclosed_by='')
 pattern='.*employeeo[1-5].csv.gz'
 on_error='SKIP_FILE'
>select * from emp_basic;

using named stage:
>create stage emp_stage;
>list @emp_stage;
>PUT file://c:\temp\employee*.csv @emp_stage;
>copy into emp_basic
 from @emp_stage
 file_format=(type=csv_field_optionally_enclosed_by='')
 pattern='.*employeeo[1-5].csv.gz'
 on_error='SKIP_FILE'
>select * from emp_basic;

/*
4. "Create an X-Small warehouse named xf_tuts_wh using the CREATE WAREHOUSE command with below options 
a) Size with x-small
b) which can be automatically suspended after 10 mins
c) setup how to automatically resume the warehouse
d) Warehouse should be suspended once after created"
*/

CREATE OR REPLACE WAREHOUSE xf_tuts_wh
    WAREHOUSE_SIZE = 'XSMALL'
    AUTO_SUSPEND = 600
    AUTO_RESUME = TRUE
    INITIALLY_SUSPENDED = TRUE;

/*
5. "A CSV file ‘customer.csv’ consists of 1 or more records, with 1 or more fields in each record, and sometimes a header record. Records and fields in each file are separated by delimiters. How will
Load the file into snowflake table ?"
*/

Step 1: 
CREATE OR REPLACE FILE FORMAT my_csv_format
  TYPE = 'CSV'
  FIELD_DELIMITER = ','
Step 2:
CREATE OR REPLACE STAGE my_stage;
(or)
CREATE OR REPLACE STAGE my_external_stage
  URL = 's3://my-bucket/path/to/files'
  CREDENTIALS = (AWS_KEY_ID = 'my_access_key_id' AWS_SECRET_KEY = 'my_secret_key');
Step 3:
COPY INTO my_table
FROM @my_stage/customer.csv  -- Replace with your stage and file path
FILE_FORMAT = (FORMAT_NAME = my_csv_format);


/*
6.	Write the commands to disable < auto-suspend > option for a virtual warehouse
*/

ALTER WAREHOUSE <warehouse_name> SET AUTO_SUSPEND = NULL;

/*
7. What is the command to concat the column named 'EMPLOYEE' between two % signs ? 
*/

SELECT CONCAT('%', EMPLOYEE, '%') AS EMPLOYEE_WITH_PERCENT
FROM your_table;

/*
8. "You have stored the below JSON in a table named car_sales as a variant column

{
  ""customer"": [
    {
      ""address"": ""San Francisco, CA"",
      ""name"": ""Joyce Ridgely"",
      ""phone"": ""16504378889""
    }
  ],
  ""date"": ""2017-04-28"",
  ""dealership"": ""Valley View Auto Sales"",
  ""salesperson"": {
    ""id"": ""55"",
    ""name"": ""Frank Beasley""
  },
  ""vehicle"": [
    {
      ""extras"": [
        ""ext warranty"",
        ""paint protection""
      ],
      ""make"": ""Honda"",
      ""model"": ""Civic"",
      ""price"": ""20275"",
      ""year"": ""2017""
    }
  ]
}
How will you query the table to get the dealership data?"
*/

CREATE OR REPLACE TABLE car_sales (
    data VARIANT
);

INSERT INTO car_sales (data)
SELECT PARSE_JSON('{
    "customer": [
        {
            "address": "San Francisco, CA",
            "name": "Joyce Ridgely",
            "phone": "16504378889"
        }
    ],
    "date": "2017-04-28",
    "dealership": "Valley View Auto Sales",
    "salesperson": {
        "id": "55",
        "name": "Frank Beasley"
    },
    "vehicle": [
        {
            "extras": [
                "ext warranty",
                "paint protection"
            ],
            "make": "Honda",
            "model": "Civic",
            "price": "20275",
            "year": "2017"
        }
    ]
}');

SELECT data:dealership AS dealership
FROM car_sales;

/*
9. A medium size warehouse runs in Auto-scale mode for 3 hours with a resize from Medium (4 servers per cluster) to Large (8 servers per cluster). Warehouse is resized from Medium to Large at 1:30 hours, Cluster 1 runs continuously, Cluster 2 runs continuously for the 2nd and 3rd hours, Cluster 3 runs for 15 minutes in the 3rd hour. How many total credits will be consumed
*/

ANSWER
1st Hour, 4 credits
2nd Hour, 2+4 + 2+4 = 12 credits
3rd hour, 8+8+2 = 18 credits
Total credits = 4 + 12 + 18 = 34 credits

/*
10	What is the command to check status of snowpipe?
*/
SELECT SYSTEM$PIPE_STATUS('Pipe_name');

SELECT * FROM SNOWFLAKE.ACCOUNT_USAGE.LOAD_HISTORY WHERE PIPE_NAME = 'Pipe_name';

SELECT pipe_name, last_pipe_status, last_load_time, last_load_duration
FROM information_schema.pipes;

SHOW PIPES;
/*
11.	What are the different methods of getting/accessing/querying data from Time travel , Assume the table name is 'CUSTOMER' and please write the command for each method.
*/
There are 3 methods : 

a) At Timestamp
SELECT * FROM CUSTOMER 
AT (TIMESTAMP => '2023-07-15 12:00:00'); 

b) Before Timestamp
SELECT * FROM CUSTOMER
BEFORE (TIMESTAMP => '2023-07-15 12:00:00');  

c) Before QuerryID 
SELECT * FROM CUSTOMER
BEFORE (QUERY_ID => 'OUR_QUERY_ID');

/*
12	If comma is defined as column delimiter in file "employee.csv" and if we get extra comma in the data how to handle this scenario?
*/

a) Alternative Delimiter: Use a different delimiter that is less likely to appear in your data, such as a tab character (\t) or pipe (|). This can avoid confusion with data containing commas.

CREATE OR REPLACE FILE FORMAT my_tab_delimited_format
  TYPE = 'CSV'
  FIELD_DELIMITER = '\t'; 

b) Quote Enclosure: Enclose fields containing commas within double quotes. Most CSV parsers will handle quoted fields correctly and ignore commas within quotes as delimiters.
eg: "John Doe", "Engineer", "New York, NY", "12345"

c) Escape Character: Use an escape character (often backslash \) before the comma to indicate that it should be treated as part of the data rather than a delimiter.
eg: John Doe, Engineer, New York\, NY, 12345

/*
13	What is the command to read data directly from S3 bucket/External/Internal Stage
*/
Step 1: create target table structure

Create or replace table table_name (
column1 datatype1, column2 datatype2, ............,column_n datatype_n
);

Step 2: 
In AWS create role with S3 Read or Full Access and get ARN_code
create S3 bucket, upload data, copy S3 URI link
Step 3:
create or replace storage integration integration_name
  type = external_stage
  storage_provider = s3
  enabled = true
  storage_aws_role_arn = 'arn:aws:iam::443852188086:role/Snowflake-role'
  storage_allowed_locations = ('s3://snowflake-demo-test-26022022/parquet/');
  
  DESC INTEGRATION integration_name;
Step 4: Update trust policy with atorage_AWS_IAM_user_role_arn and external_id
Step 5: create file format
eg:
create parquet format
create or replace file format INGEST_DATA.public.parquet_format
  type = 'parquet';
Step 6: Create external stage object
eg:
create or replace stage INGEST_DATA.public.ext_parquet_stage
  URL = 's3://snowflake-demo-test-26022022/parquet/'
  STORAGE_INTEGRATION = s3_int_parquet
  file_format = INGEST_DATA.public.parquet_format;
Step 7:
copy into target_table_name from @stage_name;

select * from target_table_name;

/*

/*
15	How is data unloaded out of Snowflake?
*/
SELECT * FROM emp_basic_local;
unload process:

Snowflake_environment_table -----> Stage ------> Local/S3bucket
copy into --> stage --> Get Command
get command downloads the file from snowflake environment

--eg. for internal stage:

copy into @%emp_basic_local
from emp_basic_local
file_format = (type = csv field_optionally_enclosed_by='"')
--on_error = 'skip_file';

get @emp_stage file://C:\temp\Employee\unload

--eg. for external stage:

edit role policy and add AmazonS3FullAccess

create or replace file format my_csv_unload_format
type = csv field_delimiter = ',' skip_header = 1 null_if = ('NULL', 'null') empty_field_as_null = true compression = gzip;

alter storage integration s3_int set  storage_allowed_locations=('s3://snowflake069/employee/','s3://snowflake069/emp_unload/','s3://snowflake069/zip_folder/')

desc integration s3_int

reconfigure trust_policy in AWS

create or replace stage my_s3_unload_stage
  storage_integration = s3_int
  url = 's3://snowflake069/emp_unload/'
  file_format = my_csv_unload_format;

copy into @my_s3_unload_stage
from
emp_ext_stage

copy into @my_s3_unload_stage/select_
from
(
  select 
  first_name,
  email 
  from
  emp_ext_stage
)

copy into @my_s3_unload_stage/parquet_
from
emp_ext_stage
FILE_FORMAT=(TYPE='PARQUET' SNAPPY_COMPRESSION=TRUE)



