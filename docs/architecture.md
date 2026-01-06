```mermaid
flowchart LR
  %% =========================
  %% LAYERS
  %% =========================

  subgraph RAW["Raw Layer (Ingestion & Landing)"]
    S3Source[(S3 Source Bucket\nHistorical global food CSV)]
    API[FastAPI on AWS App Runner\n/fetch_data (filters + pagination)]
    Producer[Python Producer (CLI)\nstream_to_kinesis.py]
    KDS[[Kinesis Data Streams\nkds_global_food_stream]]
    Firehose[[Kinesis Data Firehose\n(Reads from KDS)]]
    S3Landing[(S3 Destination Bucket\nFirehose delivery prefix by date)]
  end

  subgraph XFORM["Transformation Layer"]
    Lambda[Lambda Transform\n(JSON → CSV)]
    Stage[@S3 External Stage\nFood_Market_DB.RAW.S3_STAGE]
    Pipe[Snowpipe\nAUTO_INGEST=TRUE\nMY_SNOWPIPE]
    RawTable[(Snowflake RAW Table\nFood_Market_DB.RAW.RAW_DATA)]
    Snowpark[Snowpark Stored Procedure (Python)\nSNOWPARK_TRANSFORM()]
    CleanTable[(Snowflake CLEAN Table\nFood_Market_DB.CLEAN.CLEANSED_FOOD_DATA_NEW)]
    Task[Snowflake Task (CRON)\nSNOWPARK_TRANSFORM_TASK]
  end

  subgraph BI["Real-time / Analytics Layer"]
    BItool[Power BI / Tableau\n(connect to Snowflake CLEAN)]
  end

  %% =========================
  %% FLOWS
  %% =========================

  S3Source --> API
  API --> Producer
  Producer --> KDS
  KDS --> Firehose
  Firehose --> Lambda
  Lambda --> S3Landing

  S3Landing --> Stage
  Stage --> Pipe
  Pipe --> RawTable

  Task --> Snowpark
  Snowpark --> CleanTable
  CleanTable --> BItool

  %% =========================
  %% NOTES
  %% =========================
  EventNote{{S3 Object Created Event\n(S3 → notification channel)\ntriggers Snowpipe COPY}}
  S3Landing -.-> EventNote -.-> Pipe
```
