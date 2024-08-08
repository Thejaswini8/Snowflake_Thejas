--Snowflake chess data loading form S3 bucket

use database abc;

--Implementing Data Warehouse on Snowflake

-- creating integration
create or replace storage integration chess_integration
  type = external_stage
  storage_provider = s3
  enabled = true
  storage_aws_role_arn = 'arn:aws:iam::211125644323:role/chess-assign4-role'
  storage_allowed_locations = ('s3://chess-bucket-assign4/chess/');
  
  desc integration chess_integration;
-- updatng trust policy with STORAGE_AWS_IAM_USER_ARN and STORAGE_AWS_EXTERNAL_ID in AWS role
  
 --creating file format
  create or replace file format json_file_format
    type='json',
    strip_outer_array=TRUE;
   
  --Create external stage object
  create or replace stage chess_stage
  URL = 's3://chess-bucket-assign4/chess/'
  STORAGE_INTEGRATION = chess_integration
  file_format = json_file_format;

--create the three different tables for the data

-- Create the list_table
CREATE OR REPLACE TABLE list_table (
    username VARCHAR,
    is_live BOOLEAN
);

-- Create the info_table
CREATE OR REPLACE TABLE info_table (
    username VARCHAR,
    followers NUMERIC,
    country VARCHAR,
    joined DATE,
    location VARCHAR,
    name VARCHAR,
    player_id STRING,
    status VARCHAR,
    title VARCHAR,
    primary_key NUMERIC
);

-- Create the stats_table
CREATE OR REPLACE TABLE stats_table (
    last_blitz NUMERIC,
    draw_blitz NUMERIC,
    loss_blitz NUMERIC,
    win_blitz NUMERIC,
    last_bullet NUMERIC,
    draw_bullet NUMERIC,
    loss_bullet NUMERIC,
    win_bullet NUMERIC,
    last_rapid NUMERIC,
    draw_rapid NUMERIC,
    loss_rapid NUMERIC,
    win_rapid NUMERIC,
    FIDE NUMERIC,
    primary_key NUMERIC
);

-- Create a Snowpipe to load data from S3 into the list_table
CREATE OR REPLACE PIPE list_pipe AS
COPY INTO list_table (username, is_live)
FROM (SELECT 
        $1:username::STRING,
        $1:is_live::BOOLEAN
      FROM @chess_stage/list_file.json)
FILE_FORMAT = json_file_format;

-- Create a Snowpipe to load data from S3 into the info_table
CREATE OR REPLACE PIPE info_pipe AUTO_INGEST = TRUE AS
COPY INTO info_table (username, followers, country, joined, location, name, player_id, status, title, primary_key)
FROM (SELECT 
        $1:username::VARCHAR,-- Assuming username can be used as primary_key
        $1:followers::NUMERIC,
        $1:country::VARCHAR,
        $1:joined::DATE,
        $1:location::VARCHAR,
        $1:name::VARCHAR,
        $1:player_id::STRING,
        $1:status::VARCHAR,
        $1:title::VARCHAR
        $1:primary_key::VARCHAR
      FROM @chess_stage/Info_file.json)
FILE_FORMAT = json_file_format;

-- Create a Snowpipe to load data from S3 into the stats_table
CREATE OR REPLACE PIPE stats_pipe AUTO_INGEST = TRUE AS
COPY INTO stats_table (last_blitz, draw_blitz, loss_blitz, win_blitz, last_bullet, draw_bullet, loss_bullet, win_bullet, last_rapid, draw_rapid, loss_rapid, win_rapid, FIDE,primary_key)
FROM (
  SELECT 
    $1:last_blitz::NUMERIC AS last_blitz,
    $1:draw_blitz::NUMERIC AS draw_blitz,
    $1:loss_blitz::NUMERIC AS loss_blitz,
    $1:win_blitz::NUMERIC AS win_blitz,
    $1:last_bullet::NUMERIC AS last_bullet,
    $1:draw_bullet::NUMERIC AS draw_bullet,
    $1:loss_bullet::NUMERIC AS loss_bullet,
    $1:win_bullet::NUMERIC AS win_bullet,
    $1:last_rapid::NUMERIC AS last_rapid,
    $1:draw_rapid::NUMERIC AS draw_rapid,
    $1:loss_rapid::NUMERIC AS loss_rapid,
    $1:win_rapid::NUMERIC AS win_rapid,
    $1:FIDE::NUMERIC AS FIDE,
    $1:primary_key::NUMERIC AS primary_key 
  FROM @chess_stage/stats_file.json
)
FILE_FORMAT = json_file_format;

show pipes;

alter pipe list_pipe refresh;
alter pipe info_pipe refresh;
alter pipe stats_pipe refresh;

Select  SYSTEM$PIPE_STATUS('list_pipe');
Select  SYSTEM$PIPE_STATUS('info_pipe');
Select  SYSTEM$PIPE_STATUS('stats_pipe');

select * from list_table limit 10;
select * from info_table limit 10;
select * from stats_table limit 10;

--Results

--SQL queries to retrieve information:

--1.Username of the best player by category (blitz, chess, bullet)

-- Best blitz player
SELECT username
FROM info_table AS it
JOIN stats_table AS st ON it.primary_key = st.primary_key
ORDER BY st.last_blitz DESC
LIMIT 1;

-- Best bullet player
SELECT username
FROM info_table AS it
JOIN stats_table AS st ON it.primary_key = st.primary_key
ORDER BY st.last_bullet DESC
LIMIT 1;

-- Best rapid player
SELECT username
FROM info_table AS it
JOIN stats_table AS st ON it.primary_key = st.primary_key
ORDER BY st.last_rapid DESC
LIMIT 1;

--2.Full name (or username if null) of the best player and his FIDE elo

SELECT 
    COALESCE(it.name, it.username) AS player_name,
    st.FIDE
FROM info_table AS it
JOIN stats_table AS st ON it.primary_key = st.primary_key
ORDER BY st.FIDE DESC
LIMIT 1;

--3.Average elo of premium, staff and basic players

SELECT 
    status,
    AVG(last_blitz) AS avg_blitz_elo,
    AVG(last_bullet) AS avg_bullet_elo,
    AVG(last_rapid) AS avg_rapid_elo,
    AVG(FIDE) AS avg_FIDE_elo
FROM info_table AS it
JOIN stats_table AS st ON it.primary_key = st.primary_key
WHERE status IN ('premium', 'staff', 'basic')
GROUP BY status;

--4.Number of professional players and their elo

SELECT 
    COUNT(*) AS num_pro_players,
    AVG(FIDE) AS avg_pro_elo
FROM info_table AS it
JOIN stats_table AS st ON it.primary_key = st.primary_key
WHERE title IS NOT NULL;

--5.Average FIDE elo by their professional FIDE elo

SELECT 
    title,
    AVG(FIDE) AS avg_FIDE_elo
FROM info_table AS it
JOIN stats_table AS st ON it.primary_key = st.primary_key
GROUP BY title;

--6.Best player currently on live

SELECT 
    it.username,
    st.FIDE
FROM list_table AS lt
JOIN info_table AS it ON lt.username = it.username
JOIN stats_table AS st ON it.primary_key = st.primary_key
WHERE lt.is_live = 1
ORDER BY st.FIDE DESC
LIMIT 1;

