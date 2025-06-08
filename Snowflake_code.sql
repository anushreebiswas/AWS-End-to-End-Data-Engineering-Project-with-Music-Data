CREATE DATABASE spotify_db;

CREATE SCHEMA IF NOT EXISTS spotify_db.public;
---------------------------------------------------------------------------------------------------

CREATE OR REPLACE STORAGE INTEGRATION s3_init
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = S3
  ENABLED = TRUE
  STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::xyz:role/spotify-spark-snowflake-role'
  STORAGE_ALLOWED_LOCATIONS = ('s3://spotify-daily-data-project')
  COMMENT = 'Creating connection to S3';
