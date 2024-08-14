
Use Database BANKING;

--Implementing Data Warehouse on Snowflake

-- creating integration
create or replace storage integration BANKING_Integration
  type = external_stage
  storage_provider = s3
  enabled = true
  storage_aws_role_arn = 'arn:aws:iam::211125644323:role/capston-role-banking'
  storage_allowed_locations = ('s3://capston/banking/');
  
  desc integration BANKING_Integration;
-- updatng trust policy with STORAGE_AWS_IAM_USER_ARN and STORAGE_AWS_EXTERNAL_ID in AWS role
  
 --creating file format
  create or replace file format csv_file_format
    type='csv';
   
  --Create external stage object
  create or replace stage BANKING_Stage
  URL = 's3://capston/banking/'
  STORAGE_INTEGRATION = BANKING_Integration
  file_format = csv_file_format;
  
--create the five different tables for the data

CREATE OR REPLACE TABLE BANKING.RAW.Transactions (
    transaction_id STRING,
    customer_id STRING,
    transaction_date TIMESTAMP_NTZ,
    amount FLOAT,
    currency STRING,
    transaction_type STRING,
    channel STRING,
    merchant_name STRING,
    merchant_category STRING,
    location_country STRING,
    location_city STRING,
    is_flagged BOOLEAN
);

CREATE OR REPLACE TABLE BANKING.RAW.Customers (
    customer_id STRING,
    first_name STRING,
    last_name STRING,
    date_of_birth DATE,
    gender STRING,
    email STRING,
    phone_number STRING,
    address STRING,
    city STRING,
    country STRING,
    occupation STRING,
    income_bracket STRING,
    customer_since DATE
);

CREATE OR REPLACE TABLE BANKING.RAW.Accounts (
    account_id STRING,
    customer_id STRING,
    account_type STRING,
    account_status STRING,
    open_date DATE,
    current_balance FLOAT,
    currency STRING,
    credit_limit FLOAT
);

CREATE OR REPLACE TABLE BANKING.RAW.Credit_Data (
    customer_id STRING,
    credit_score INT,
    number_of_credit_accounts INT,
    total_credit_limit FLOAT,
    total_credit_used FLOAT,
    number_of_late_payments INT,
    bankruptcies INT
);

CREATE OR REPLACE TABLE BANKING.RAW.Watchlist (
    entity_id STRING,
    entity_name STRING,
    entity_type STRING,
    risk_category STRING,
    listed_date DATE,
    source STRING
);

-- Create Pipes

CREATE OR REPLACE PIPE Transaction_pipe auto_ingest=TRUE
    AS
    COPY INTO BANKING.RAW.Transactions
    FROM @BANKING_Stage/transactions.csv
    FILE_FORMAT = csv_file_format
    on_error=continue;
    
CREATE OR REPLACE PIPE Customer_pipe auto_ingest=TRUE
    AS
    COPY INTO BANKING.RAW.Customers
    FROM @BANKING_Stage/customers.csv
    FILE_FORMAT = csv_file_format
    on_error=continue;

CREATE OR REPLACE PIPE Account_pipe auto_ingest=TRUE
    AS
    COPY INTO BANKING.RAW.Accounts
    FROM @BANKING_Stage/accounts.csv
    FILE_FORMAT = csv_file_format
    on_error=continue;

CREATE OR REPLACE PIPE Credit_data_pipe auto_ingest=TRUE
    AS
    COPY INTO BANKING.RAW.Credit_Data
    FROM @BANKING_Stage/credit_data.csv
    FILE_FORMAT = csv_file_format
    on_error=continue;

CREATE OR REPLACE PIPE Watchlist_pipe auto_ingest=TRUE
    AS
    COPY INTO BANKING.RAW.watchlist
    FROM @BANKING_Stage/watchlist.csv
    FILE_FORMAT = csv_file_format
    on_error=continue;
    

SHOW PIPES;
--CONFIGURE SQS

desc pipe Transaction_pipe;

alter pipe Transaction_pipe refresh;
alter pipe Customer_pipe refresh;
alter pipe Account_pipe refresh;
alter pipe Credit_data_pipe refresh;
alter pipe Watchlist_pipe refresh;

--Check Pipe Status

Select  SYSTEM$PIPE_STATUS('Transaction_pipe');
Select  SYSTEM$PIPE_STATUS('Customer_pipe');
Select  SYSTEM$PIPE_STATUS('Account_pipe');
Select  SYSTEM$PIPE_STATUS('Credit_data_pipe');
Select  SYSTEM$PIPE_STATUS('Watchlist_pipe');

SELECT * FROM INFORMATION_SCHEMA.LOAD_HISTORY WHERE PIPE_NAME = 'Transaction_pipe';
select * from BANKING.RAW.Transactions limit 10;


--Creating Streams

CREATE OR REPLACE STREAM Transactions_Stream 
ON TABLE BANKING.RAW.transactions;

CREATE OR REPLACE STREAM Customers_Stream 
ON TABLE BANKING.RAW.customers;

CREATE OR REPLACE STREAM Accounts_Stream 
ON TABLE BANKING.RAW.accounts;

CREATE OR REPLACE STREAM Credit_Data_Stream 
ON TABLE BANKING.RAW.credit_data;

CREATE OR REPLACE STREAM Watchlist_Stream 
ON TABLE BANKING.RAW.watchlist;

--Creating Tasks

CREATE OR REPLACE TASK Transaction_Task 
WAREHOUSE='BANK_IMPORT' 
SCHEDULE='1 minute'
WHEN SYSTEM$STREAM_HAS_DATA('Transactions_Stream') AS
CALL Merge_Transactions();

CREATE OR REPLACE TASK Customer_Task 
WAREHOUSE='BANK_IMPORT' 
SCHEDULE='1 minute'
WHEN SYSTEM$STREAM_HAS_DATA('Customers_Stream') AS
CALL Merge_Customers();

CREATE OR REPLACE TASK Accounts_Task 
WAREHOUSE='BANK_IMPORT' 
SCHEDULE='1 minute'
WHEN SYSTEM$STREAM_HAS_DATA('Accounts_Stream') AS
CALL Merge_Accounts();

CREATE OR REPLACE TASK Credit_Data_Task 
WAREHOUSE='BANK_IMPORT' 
SCHEDULE='1 minute'
WHEN SYSTEM$STREAM_HAS_DATA('Credit_Data_Stream') AS
CALL Merge_Credit_Data();

CREATE OR REPLACE TASK Watchlist_Task 
WAREHOUSE='BANK_IMPORT' 
SCHEDULE='1 minute'
WHEN SYSTEM$STREAM_HAS_DATA('Watchlist_Stream') AS
CALL Merge_Watchlist();


ALTER TASK Transaction_Task RESUME;
ALTER TASK Customer_Task RESUME;
ALTER TASK Accounts_Task RESUME;
ALTER TASK Credit_Data_Task RESUME;
ALTER TASK Watchlist_Task RESUME;

--Creating Stored Procedures

CREATE OR REPLACE PROCEDURE Merge_Transactions()
RETURNS STRING
LANGUAGE SQL
AS
$$
MERGE INTO transactions tgt
USING transactions_stream src
ON tgt.transaction_id = src.transaction_id
WHEN MATCHED AND src.metadata$action = 'DELETE' THEN
    DELETE
WHEN MATCHED AND src.metadata$action = 'UPDATE' THEN
    UPDATE SET
        customer_id = src.customer_id,
        transaction_date = src.transaction_date,
        amount = src.amount,
        currency = src.currency,
        transaction_type = src.transaction_type,
        channel = src.channel,
        merchant_name = src.merchant_name,
        merchant_category = src.merchant_category,
        location_country = src.location_country,
        location_city = src.location_city,
        is_flagged = src.is_flagged
WHEN NOT MATCHED THEN
    INSERT (transaction_id, customer_id, transaction_date, amount, currency, 
            transaction_type, channel, merchant_name, merchant_category, 
            location_country, location_city, is_flagged)
    VALUES (src.transaction_id, src.customer_id, src.transaction_date, src.amount, 
            src.currency, src.transaction_type, src.channel, src.merchant_name, 
            src.merchant_category, src.location_country, src.location_city, src.is_flagged);
$$;

CREATE OR REPLACE PROCEDURE Merge_Customers()
RETURNS STRING
LANGUAGE SQL
AS
$$
MERGE INTO customers tgt
USING customers_stream src
ON tgt.customer_id = src.customer_id
WHEN MATCHED AND src.metadata$action = 'DELETE' THEN
    DELETE
WHEN MATCHED AND src.metadata$action = 'UPDATE' THEN
    UPDATE SET
        first_name = src.first_name,
        last_name = src.last_name,
        date_of_birth = src.date_of_birth,
        gender = src.gender,
        email = src.email,
        phone_number = src.phone_number,
        address = src.address,
        city = src.city,
        country = src.country,
        occupation = src.occupation,
        income_bracket = src.income_bracket,
        customer_since = src.customer_since
WHEN NOT MATCHED THEN
    INSERT (customer_id, first_name, last_name, date_of_birth, gender, email, phone_number, address, city, country, occupation, income_bracket, customer_since)
    VALUES (src.customer_id, src.first_name, src.last_name, src.date_of_birth, src.gender, src.email, src.phone_number, src.address, src.city, src.country, src.occupation, src.income_bracket, src.customer_since);
$$;

CREATE OR REPLACE PROCEDURE Merge_Accounts()
RETURNS STRING
LANGUAGE SQL
AS
$$
MERGE INTO accounts tgt
USING accounts_stream src
ON tgt.account_id = src.account_id
WHEN MATCHED AND src.metadata$action = 'DELETE' THEN
    DELETE
WHEN MATCHED AND src.metadata$action = 'UPDATE' THEN
    UPDATE SET
        customer_id = src.customer_id,
        account_type = src.account_type,
        account_status = src.account_status,
        open_date = src.open_date,
        current_balance = src.current_balance,
        currency = src.currency,
        credit_limit = src.credit_limit
WHEN NOT MATCHED THEN
    INSERT (account_id, customer_id, account_type, account_status, open_date, current_balance, currency, credit_limit)
    VALUES (src.account_id, src.customer_id, src.account_type, src.account_status, src.open_date, src.current_balance, src.currency, src.credit_limit);
$$;

CREATE OR REPLACE PROCEDURE Merge_Credit_Data()
RETURNS STRING
LANGUAGE SQL
AS
$$
MERGE INTO credit_data tgt
USING credit_data_stream src
ON tgt.customer_id = src.customer_id
WHEN MATCHED AND src.metadata$action = 'DELETE' THEN
    DELETE
WHEN MATCHED AND src.metadata$action = 'UPDATE' THEN
    UPDATE SET
        credit_score = src.credit_score,
        number_of_credit_accounts = src.number_of_credit_accounts,
        total_credit_limit = src.total_credit_limit,
        total_credit_used = src.total_credit_used,
        number_of_late_payments = src.number_of_late_payments,
        bankruptcies = src.bankruptcies
WHEN NOT MATCHED THEN
    INSERT (customer_id, credit_score, number_of_credit_accounts, total_credit_limit, total_credit_used, number_of_late_payments, bankruptcies)
    VALUES (src.customer_id, src.credit_score, src.number_of_credit_accounts, src.total_credit_limit, src.total_credit_used, src.number_of_late_payments, src.bankruptcies);
$$;

CREATE OR REPLACE PROCEDURE Merge_Watchlist()
RETURNS STRING
LANGUAGE SQL
AS
$$
MERGE INTO watchlist tgt
USING watchlist_stream src
ON tgt.entity_id = src.entity_id
WHEN MATCHED AND src.metadata$action = 'DELETE' THEN
    DELETE
WHEN MATCHED AND src.metadata$action = 'UPDATE' THEN
    UPDATE SET
        entity_name = src.entity_name,
        entity_type = src.entity_type,
        risk_category = src.risk_category,
        listed_date = src.listed_date,
        source = src.source
WHEN NOT MATCHED THEN
    INSERT (entity_id, entity_name, entity_type, risk_category, listed_date, source)
    VALUES (src.entity_id, src.entity_name, src.entity_type, src.risk_category, src.listed_date, src.source);
$$;

