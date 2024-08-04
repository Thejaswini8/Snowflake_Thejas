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
    player_id NUMERIC,
    status VARCHAR,
    title VARCHAR,
    player_id NUMERIC
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
    player_id NUMERIC
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
CREATE OR REPLACE PIPE info_pipe AS
COPY INTO info_table (username, followers, country, joined, location, name, player_id, status, title, primary_key)
FROM (SELECT 
        $1:username::STRING,
        $1:followers::NUMERIC,
        $1:country::STRING,
        TO_DATE($1:joined::STRING, 'YYYY-MM-DD'),
        $1:location::STRING,
        $1:name::STRING,
        $1:player_id::NUMERIC,-- Assuming player_id can be used as primary_key
        $1:status::STRING,
        $1:title::STRING
      FROM @chess_stage/info_file.json)
FILE_FORMAT = json_file_format;

-- Create a Snowpipe to load data from S3 into the stats_table
CREATE OR REPLACE PIPE stats_pipe AS
COPY INTO stats_table (last_blitz, draw_blitz, loss_blitz, win_blitz, last_bullet, draw_bullet, loss_bullet, win_bullet, last_rapid, draw_rapid, loss_rapid, win_rapid, FIDE, primary_key)
FROM (
  SELECT 
    $1:chess_blitz:last:rating::NUMERIC AS last_blitz,
    $1:chess_blitz:record:draw::NUMERIC AS draw_blitz,
    $1:chess_blitz:record:loss::NUMERIC AS loss_blitz,
    $1:chess_blitz:record:win::NUMERIC AS win_blitz,
    $1:chess_bullet:last:rating::NUMERIC AS last_bullet,
    $1:chess_bullet:record:draw::NUMERIC AS draw_bullet,
    $1:chess_bullet:record:loss::NUMERIC AS loss_bullet,
    $1:chess_bullet:record:win::NUMERIC AS win_bullet,
    $1:chess_rapid:last:rating::NUMERIC AS last_rapid,
    $1:chess_rapid:record:draw::NUMERIC AS draw_rapid,
    $1:chess_rapid:record:loss::NUMERIC AS loss_rapid,
    $1:chess_rapid:record:win::NUMERIC AS win_rapid,
    $1:fide::NUMERIC AS FIDE,
    $1:player_id::NUMERIC AS primary_key -- Assuming player_id can be used as primary_key
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

-- Best Blitz Player
SELECT username, MAX(last_blitz) AS max_blitz_rating
FROM stats_table
GROUP BY username
ORDER BY max_blitz_rating DESC
LIMIT 1;

-- Best Bullet Player
SELECT username, MAX(last_bullet) AS max_bullet_rating
FROM stats_table
GROUP BY username
ORDER BY max_bullet_rating DESC
LIMIT 1;

-- Best Rapid Player
SELECT username, MAX(last_rapid) AS max_rapid_rating
FROM stats_table
GROUP BY username
ORDER BY max_rapid_rating DESC
LIMIT 1;

--2.Full name (or username if null) of the best player and his FIDE elo

SELECT 
    COALESCE(p.name, p.username) AS best_player_name, 
    s.FIDE AS fide_elo
FROM info_table p
JOIN stats_table s ON p.username = s.username
ORDER BY s.FIDE DESC
LIMIT 1;

--3.Average elo of premium, staff and basic players

SELECT 
    status,
    AVG(s.FIDE) AS average_fide_elo
FROM info_table p
JOIN stats_table s ON p.username = s.username
WHERE p.status IN ('premium', 'staff', 'basic')
GROUP BY status;

--4.Number of professional players and their elo

SELECT 
    COUNT(p.username) AS professional_player_count,
    AVG(s.FIDE) AS average_fide_elo
FROM info_table p
JOIN stats_table s ON p.username = s.username
WHERE p.title IS NOT NULL;  -- Assuming professional players have a non-null title

--5.Average FIDE elo by their professional FIDE elo

SELECT 
    title,
    AVG(s.FIDE) AS average_fide_elo
FROM info_table p
JOIN stats_table s ON p.username = s.username
WHERE p.title IS NOT NULL
GROUP BY title;

--6.Best player currently on live

SELECT 
    l.username,
    s.FIDE AS fide_elo
FROM list_table l
JOIN stats_table s ON l.username = s.username
WHERE l.is_live = TRUE
ORDER BY s.FIDE DESC
LIMIT 1;
