# Global Food Market – Real-Time Data Streaming Pipeline

## Overview
This project demonstrates an end-to-end real-time data engineering pipeline built using AWS and Snowflake.

Global food market data is exposed via an API, streamed through AWS Kinesis, transformed and delivered to Amazon S3 using Kinesis Firehose, and automatically ingested into Snowflake for downstream processing and analytics. The pipeline is designed to showcase real-time ingestion, event-driven loading, and in-warehouse transformations using Snowpark.

## Architecture
The pipeline is implemented using a layered architecture to clearly separate data ingestion, transformation, and analytics concerns.

In the **Raw (Ingestion & Landing) layer**, historical food market data stored in Amazon S3 is exposed through a FastAPI application deployed on AWS App Runner. A Python-based producer consumes this API and streams records into Amazon Kinesis Data Streams. Kinesis Firehose reads from the stream, applies an inline AWS Lambda transformation to convert JSON records into CSV format, and writes the transformed data to a destination S3 bucket.

In the **Transformation layer**, Snowflake automatically ingests newly landed files from the S3 destination bucket using Snowpipe with event-driven auto-ingestion. Data is first loaded into RAW tables and then transformed into cleaned, analytics-ready tables using Snowpark (Python) stored procedures. These transformations are scheduled and orchestrated using Snowflake Tasks.

The resulting CLEAN tables can be directly consumed by BI tools such as Power BI or Tableau for reporting and analysis.

See `docs/architecture.md` for a detailed architecture diagram and component breakdown.

## Tech Stack
- AWS App Runner (FastAPI)
- AWS Kinesis Data Streams
- AWS Kinesis Firehose
- AWS Lambda
- Amazon S3
- Snowflake (Snowpipe, Snowpark)
- Python

## Repository Structure

```text
global-food-market-data-streaming-project/
├── app/                         # FastAPI application (App Runner)
│   ├── main.py                  # /fetch_data API endpoint
│   └── requirements.txt
│
├── src/
│   └── producers/
│       └── stream_to_kinesis.py # API → Kinesis producer (CLI)
│
├── lambda/
│   └── firehose_transform/
│       └── lambda_function.py   # Firehose inline transform (JSON → CSV)
│
├── snowflake/
│   ├── setup/                   # Database, schemas, file formats, stages
│   ├── ingestion/               # RAW tables and Snowpipe definitions
│   ├── transform/               # Snowpark stored procedures
│   ├── task/                    # Snowflake Tasks (CRON scheduling)
│   └── validation/              # Data quality & verification queries
│
├── docs/
│   └── architecture.md          # Architecture diagram & explanation
│
├── README.md
├── LICENSE
└── .gitignore
```

## How to Run
See docs/run-the-pipeline.md

## Production Considerations
- In production, the producer would be triggered via EventBridge / ECS
- Manual execution is used here for cost control and demonstration
