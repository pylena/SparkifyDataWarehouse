# Sparkify Data Warehouse 

The goal of this project is to build an ETL pipeline that extracts data from S3, stages it in Amazon Redshift, and transforms it into a set of dimensional tables optimized for analytical queries. Sparkify, a music streaming startup, needs to analyze user listening behavior and understand their music preferences.

## Dataset

* Log Data: JSON logs of user activity (e.g., song plays).

* Song Data: JSON metadata about songs and artists.

## Project Architecture
<img src="https://github.com/user-attachments/assets/aaad3611-8409-4855-b4f5-10e0e58413ea" width="600"/>

## Sparkfy Star Schema Database
<img src="https://github.com/user-attachments/assets/f10df04d-caac-43d6-8234-f7c5b037b730" width="600"/>


## How to Run the Project

- Step 1: Configure AWS Resources
  * Launch a Redshift cluster and IAM role with S3 read access.
  * Update the dwh.cfg file with your resources Info.

- Step 2: Set Up the Tables
  * Run : python create_tables.py

- Step 3: Run the ETL Pipeline
  * Run: python etl.py

### Repository File Structure
| File               | Description                                                                    |
| ------------------ | ------------------------------------------------------------------------------ |
| `create_tables.py` | Connects to Redshift and creates all necessary tables.                         |
| `etl.py`           | Runs the ETL pipeline: loads staging tables and inserts into analytics tables. |
| `sql_queries.py`   | Contains all SQL queries.                      |
| `dwh.cfg`          | Configuration file with AWS credentials, Redshift, and S3 paths.               |





                                        

  





