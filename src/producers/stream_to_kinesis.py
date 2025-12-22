"""
stream_to_kinesis.py

Streams paginated records from your FastAPI endpoint to Kinesis Data Streams.

API response format (confirmed):
{
  "data": [ {...}, {...}, ... ],
  "next_offset": 100
}

Example request:
  /fetch_data?year=2008&country=Sri%20Lanka&offset=0&limit=100
"""

import argparse
import json
import logging
import os
import time
from typing import Any, Dict, List, Optional

import boto3
import requests

logger = logging.getLogger("api_to_kds")


def build_kinesis_client(region: str):
    # Uses AWS default credential chain (AWS profile, env vars, IAM role, etc.)
    return boto3.client("kinesis", region_name=region)


def fetch_page(
    api_url: str,
    year: int,
    country: str,
    offset: int,
    limit: int,
) -> Dict[str, Any]:
    """
    Calls the API once and returns parsed JSON.
    Expected keys: 'data' (list), 'next_offset' (int, optional)
    """
    params = {
        "year": year,
        "country": country,
        "offset": offset,
        "limit": limit,
    }

    resp = requests.get(api_url, params=params, timeout=60)
    resp.raise_for_status()

    payload = resp.json()

    # Validate expected structure early (fail fast if API changes)
    if not isinstance(payload, dict) or "data" not in payload:
        raise ValueError(f"Unexpected API response structure. Got keys: {list(payload) if isinstance(payload, dict) else type(payload)}")

    if not isinstance(payload["data"], list):
        raise ValueError(f"Unexpected 'data' type. Expected list, got {type(payload['data'])}")

    return payload


def put_records_batch(
    kinesis_client,
    stream_name: str,
    items: List[Dict[str, Any]],
    partition_key_field: Optional[str] = None,
    default_partition_key: str = "1",
):
    """
    Sends up to 500 records per request using PutRecords.
    If partition_key_field is provided and exists in the record, uses it.
    Otherwise uses default_partition_key.

    NOTE: Using a more varied partition key is better for shard distribution.
    """
    if not items:
        return

    records = []
    for item in items:
        if not isinstance(item, dict):
            # Skip or raise; choose raise to avoid corrupting stream
            raise ValueError(f"Expected each record in 'data' to be a dict, got {type(item)}")

        if partition_key_field and partition_key_field in item and item[partition_key_field] not in (None, ""):
            pk = str(item[partition_key_field])
        else:
            pk = default_partition_key

        records.append(
            {
                "Data": json.dumps(item, ensure_ascii=False),
                "PartitionKey": pk,
            }
        )

    resp = kinesis_client.put_records(StreamName=stream_name, Records=records)

    failed = resp.get("FailedRecordCount", 0)
    if failed and failed > 0:
        # In real pipelines you’d retry failed ones; for now fail loudly so you notice.
        raise RuntimeError(f"PutRecords failed for {failed} record(s). Response: {resp}")


def chunked(items: List[Dict[str, Any]], size: int):
    for i in range(0, len(items), size):
        yield items[i : i + size]


def stream_all_pages(
    api_url: str,
    kinesis_client,
    stream_name: str,
    year: int,
    country: str,
    start_offset: int,
    limit: int,
    sleep_between_pages_sec: float = 0.0,
):
    """
    Keeps calling the API using next_offset until no more data is returned.
    """
    offset = start_offset
    total_sent = 0

    while True:
        logger.info("Fetching page offset=%s limit=%s year=%s country=%s", offset, limit, year, country)

        payload = fetch_page(api_url, year=year, country=country, offset=offset, limit=limit)
        rows = payload.get("data", [])
        next_offset = payload.get("next_offset", None)

        if not rows:
            logger.info("No rows returned. Stopping.")
            break

        # Send records to Kinesis (in Kinesis max batch size 500)
        for batch in chunked(rows, 500):
            # Partition key suggestion:
            # - better: "mkt_name" (or "country" + "mkt_name") to spread load
            put_records_batch(
                kinesis_client,
                stream_name=stream_name,
                items=batch,
                partition_key_field="mkt_name",   # you have this field
                default_partition_key=country,    # fallback spreads by country at least
            )

        total_sent += len(rows)
        logger.info("Sent %s record(s) this page. Total sent=%s", len(rows), total_sent)

        if next_offset is None:
            logger.info("No next_offset in response. Stopping.")
            break

        # Advance pagination
        offset = int(next_offset)

        if sleep_between_pages_sec > 0:
            time.sleep(sleep_between_pages_sec)

    return total_sent


def main():
    parser = argparse.ArgumentParser(description="Stream FastAPI paginated data to Kinesis Data Streams")
    parser.add_argument("--api-url", required=True, help="Full API URL, e.g. https://.../fetch_data")
    parser.add_argument("--stream-name", default="kds_global_food_stream", help="Kinesis Data Stream name")
    parser.add_argument("--region", default=os.getenv("AWS_REGION", "us-east-2"), help="AWS region (App Runner region, us-east-2 in this case)")
    parser.add_argument("--year", type=int, required=True, help="Year filter, e.g. 2008")
    parser.add_argument("--country", required=True, help="Country filter, e.g. 'Sri Lanka'")
    parser.add_argument("--start-offset", type=int, default=0, help="Starting offset (default 0)")
    parser.add_argument("--limit", type=int, default=100, help="Page size (default 100)")
    parser.add_argument("--sleep-between-pages-sec", type=float, default=0.0, help="Optional sleep between API page requests")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    kinesis_client = build_kinesis_client(args.region)

    total = stream_all_pages(
        api_url=args.api_url,
        kinesis_client=kinesis_client,
        stream_name=args.stream_name,
        year=args.year,
        country=args.country,
        start_offset=args.start_offset,
        limit=args.limit,
        sleep_between_pages_sec=args.sleep_between_pages_sec,
    )

    logger.info("DONE. Total records streamed to KDS: %s", total)


if __name__ == "__main__":
    main()
