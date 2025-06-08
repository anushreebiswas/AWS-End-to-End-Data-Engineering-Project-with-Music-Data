
---> Create database

CREATE DATABASE spotify_db;

CREATE SCHEMA IF NOT EXISTS spotify_db.public;
---------------------------------------------------------------------------------------------------
---> Create Storage Integration
CREATE OR REPLACE STORAGE INTEGRATION s3_init
TYPE = EXTERNAL_STAGE
STORAGE_PROVIDER = S3
ENABLED = TRUE
STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::xyz:role/spotify-spark-snowflake-role'
STORAGE_ALLOWED_LOCATIONS = ('s3://spotify-daily-data-project')
COMMENT = 'Creating connection to S3';


---------------------------------------------------------------------------------------------------
---> Create File Format
CREATE OR REPLACE FILE FORMAT csv_fileformat
TYPE = 'CSV'
FIELD_DELIMITER = ','
SKIP_HEADER = 1
NULL_IF = ('NULL', 'null')
EMPTY_FIELD_AS_NULL = TRUE;

---------------------------------------------------------------------------------------------------
---> Create External Stage

CREATE OR REPLACE STAGE spotify_stage
URL = 's3://spotify-daily-data-project/transformed_data/'
STORAGE_INTEGRATION = s3_init
FILE_FORMAT = csv_fileformat;



---------------------------------------------------------------------------------------------------
---> Create Tables

-- Album Table
CREATE OR REPLACE TABLE tbl_album (
  album_id STRING,
  name STRING,
  release_date DATE,
  total_tracks INT,
  url STRING
);

-- Artist Table
CREATE OR REPLACE TABLE tbl_artists (
  artist_id STRING,
  name STRING,
  url STRING
);

-- Songs Table
CREATE OR REPLACE TABLE tbl_songs (
  song_id STRING,
  song_name STRING,
  duration_ms INT,
  url STRING,
  popularity INT,
  song_added DATE,
  album_id STRING,
  artist_id STRING
);

---------------------------------------------------------------------------------------------------
---> Separate schema for Snowpipes

CREATE OR REPLACE SCHEMA pipe;

---> Create Pipes

-- Pipe for tbl_album
CREATE OR REPLACE PIPE pipe.tbl_album_pipe
  AUTO_INGEST = TRUE
AS
  COPY INTO spotify_db.public.tbl_album
  FROM @spotify_db.public.spotify_stage/album;

-- Pipe for tbl_artists
CREATE OR REPLACE PIPE pipe.tbl_artists_pipe
  AUTO_INGEST = TRUE
AS
  COPY INTO spotify_db.public.tbl_artists
  FROM @spotify_db.public.spotify_stage/artist;

-- Pipe for tbl_songs
CREATE OR REPLACE PIPE pipe.tbl_songs_pipe
  AUTO_INGEST = TRUE
AS
  COPY INTO spotify_db.public.tbl_songs
  FROM @spotify_db.public.spotify_stage/songs;

