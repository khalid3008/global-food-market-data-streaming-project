# Running the Pipeline

This document describes how to execute each stage of the pipeline end-to-end.

## 1. Deploy the API (App Runner)
- FastAPI app is located in `app/main.py`
- Deploy using AWS App Runner (source: GitHub)
- Exposes `/fetch_data` endpoint

## 2. Stream data into Kinesis
Run the producer locally or from a VM:

```powershell
python -m src.producers.stream_to_kinesis `
  --api-url "<APPRUNNER_URL>/fetch_data" `
  --stream-name "kds_global_food_stream" `
  --region "us-east-2" `
  --year 2011 `
  --country "Armenia" `
  --limit 100
