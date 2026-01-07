# Running the Pipeline

This document describes how to execute the global food market real-time data pipeline end-to-end.
The pipeline is designed so that ingestion, transformation, and analytics can be verified independently at each stage.

---

## 1. Deploy the API (FastAPI on AWS App Runner)

The FastAPI application is located in `app/main.py` and exposes a paginated `/fetch_data` endpoint that reads data from the source S3 bucket.

Steps:
- Create an AWS App Runner service using this GitHub repository as the source.
- Configure the service to run the FastAPI app (entry point: `app/main.py`).
- After deployment, note the App Runner service URL.

Example endpoint:
https://<apprunner-service-url>/fetch_data

---

## 2. Stream Data into Kinesis Data Streams

Data is streamed into AWS Kinesis using the Python producer located in `src/producers/stream_to_kinesis.py`.
The producer pulls data from the FastAPI endpoint and writes records into Kinesis Data Streams.

Example PowerShell command:

python -m src.producers.stream_to_kinesis `
  --api-url "https://<apprunner-service-url>/fetch_data" `
  --stream-name "kds_global_food_stream" `
  --region "us-east-2" `
  --year 2011 `
  --country "Armenia" `
  --start-offset 0 `
  --limit 100

Notes:
- `year` and `country` control which subset of data is streamed.
- `offset` and `limit` enable pagination.
- In a production setup, this producer would typically be scheduled (for example, using EventBridge or ECS).

---

## 3. Firehose Delivery and Lambda Transformation

- Amazon Kinesis Firehose reads records from Kinesis Data Streams.
- An inline AWS Lambda transformation function (`lambda/firehose_transform/lambda_function.py`) converts streaming JSON records into CSV format.
- Firehose delivers the transformed CSV files into the destination S3 bucket using date-based partitioning.

At this stage, data is fully landed in S3 and ready for warehouse ingestion.

---

## 4. Snowflake Ingestion with Snowpipe

Snowflake ingests data from the Firehose destination bucket using Snowpipe.

Process:
- An external stage in Snowflake points to the Firehose destination S3 bucket.
- Snowpipe is configured with AUTO_INGEST = TRUE.
- When new objects are created in S3, event notifications trigger Snowpipe.
- Snowpipe executes a COPY INTO operation to load data into the RAW table.

You can verify ingestion by querying the RAW table in Snowflake.

---

## 5. Snowflake Transformations (Snowpark)

Data loaded into the RAW table is transformed using Snowpark (Python) stored procedures.

Key points:
- Snowpark procedures clean, filter, and standardize RAW data.
- Transformed results are written to CLEAN tables.
- Execution is orchestrated using Snowflake Tasks scheduled via CRON (for example, daily runs).

Transformations can be executed manually or allowed to run on schedule.

---

## 6. Analytics and Consumption

The CLEAN tables in Snowflake represent the final, analytics-ready dataset.

These tables can be connected directly to BI tools such as:
- Power BI
- Tableau

Using native Snowflake connectors, dashboards and reports can be built on top of the CLEAN data with automated refreshes.

---

## End-to-End Flow Summary

1. Data is exposed via FastAPI from an S3 source bucket.
2. A Python producer streams data into Kinesis Data Streams.
3. Firehose applies a Lambda transform and delivers CSV files to S3.
4. Snowpipe auto-ingests new files into Snowflake RAW tables.
5. Snowpark transforms RAW data into CLEAN tables.
6. BI tools consume CLEAN data for reporting and analysis.
