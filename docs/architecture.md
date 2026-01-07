## Architecture Overview
<img width="905" height="889" alt="Untitled Diagram drawio (2)" src="https://github.com/user-attachments/assets/a7bba4c0-7ee7-4146-ae00-088506c84bac" />

This project implements a layered, real-time data ingestion and processing pipeline using AWS and Snowflake to analyze global food market data.

The architecture is divided into three logical layers: **Raw (Ingestion & Landing)**, **Transformation**, and **Analytics**.

In the Raw layer, historical global food market data is stored in Amazon S3 and exposed through a FastAPI application deployed on AWS App Runner. A Python-based producer consumes this API, streams records into Amazon Kinesis Data Streams, and delivers them to Amazon Kinesis Firehose. Firehose applies an inline AWS Lambda transformation to convert streaming JSON records into CSV format and writes the transformed data into a destination S3 bucket.

In the Transformation layer, Snowflake ingests newly landed CSV files from the S3 destination bucket using Snowpipe with event-driven auto-ingestion. The data is first loaded into a RAW table and then transformed using Snowpark (Python) stored procedures to produce cleaned, analytics-ready tables. These transformations are orchestrated using Snowflake Tasks scheduled via CRON.

The cleaned data can be directly consumed by BI tools such as Power BI or Tableau for reporting and analysis.

