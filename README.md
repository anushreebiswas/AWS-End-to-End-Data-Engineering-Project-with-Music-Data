# Spotify Global Top 50 ETL Pipeline

![Domain](https://img.shields.io/badge/Domain-Entertainment-blue?style=for-the-badge)  
![Tech](https://img.shields.io/badge/Tech-%20Python%20%7CAWS%20Cloudwatch%20%7C%20Lambda%20%7C%20Glue%20%7C%20Spark%20%7C%20Snowflake%20%7C-green?style=for-the-badge)  
![Project Type](https://img.shields.io/badge/Type-Data%20Engineering-yellow?style=for-the-badge)

## Overview

This project builds an automated ETL pipeline to collect weekly global Top 50 song data from Topsify (A channel in Spotify) from the Spotify API and store it in Snowflake using AWS services. The pipeline is designed for a client interested in tracking global music trends over time to gain insights for data-driven content creation in the music industry.

## Project Value

By collecting data every week over a year, the client will be able to uncover patterns related to trending artists, genres, and albums. This will allow them to understand what makes a song successful and make data-informed decisions when creating new music content.


## Architecture Diagram


<img width="729" alt="Screenshot 2025-06-07 at 11 48 50 PM" src="https://github.com/user-attachments/assets/74506d9b-531f-4048-989f-123f65312137" />



## Pipeline Description

### 🔄 **Extract**

- **Source**: Spotify Top 50 Global Playlist (Topsify)
- **Trigger**: Weekly via Amazon CloudWatch
- **Lambda**: Python-based function uses the Spotify API to extract current playlist data and tranforms it into json format.
- **Raw Data Storage**: Stored in Amazon S3 as json

### 🔁 **Transform**

- **Trigger**: S3 Object PUT triggers the transformation
- **AWS Glue**: Spark job performs data cleaning and transformation on raw JSON
- **Output**: Transformed data stored back into Amazon S3 as csv

### 📥 **Load**

- **Snowpipe**: Automatically ingests the transformed data from S3
- **Snowflake**: Stores structured and queryable song data for downstream analysis

## Technologies Used

- **Spotify API** (Data Source)
- **AWS Lambda** (ETL Trigger + Extraction in Python)
- **AWS CloudWatch** (Trigger for Lambda)
- **AWS S3** (Raw and Transformed Data Storage)
- **AWS Glue** (Apache Spark-based Transformation)
- **Snowpipe & Snowflake** (Data Warehouse & Auto-loading)

## Key Features

- Fully serverless and scalable architecture
- Collects weekly updates without manual intervention
- Enables year-long data accumulation for rich analytics





