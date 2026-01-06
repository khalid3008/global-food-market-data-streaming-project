# Global Food Market – Real-Time Data Streaming Pipeline

## Overview
This project demonstrates an end-to-end real-time data engineering pipeline using AWS and Snowflake.

The pipeline ingests global food market data via an API, streams it through AWS Kinesis, delivers it to Amazon S3 using Firehose with Lambda-based transformation, auto-ingests into Snowflake using Snowpipe, and performs in-warehouse transformations using Snowpark. The cleaned data is designed for downstream BI tools such as Power BI.

## Architecture
[diagram]
See docs/architecture.md

## Tech Stack
- AWS App Runner (FastAPI)
- AWS Kinesis Data Streams
- AWS Kinesis Firehose
- AWS Lambda
- Amazon S3
- Snowflake (Snowpipe, Snowpark)
- Python

## Repository Structure
[brief tree]

## How to Run
See docs/run-the-pipeline.md

## Verification
See docs/verify-each-stage.md

## Production Considerations
- In production, the producer would be triggered via EventBridge / ECS
- Manual execution is used here for cost control and demonstration
