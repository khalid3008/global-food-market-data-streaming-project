from typing import Optional, List, Any

from fastapi import FastAPI, Query, HTTPException
import pandas as pd
from io import StringIO
import uvicorn
import requests

# FastAPI application for fetching and filtering data from a CSV file in S3
app = FastAPI()

S3_CSV_URL = "https://khalid-global-food-market-data-raw.s3.us-east-2.amazonaws.com/total_data.csv"


def fetch_data(
    year: Optional[int] = None,
    country: Optional[str] = None,
    market: Optional[str] = None,
) -> pd.DataFrame:
    """
    Fetch and filter data from a CSV file hosted on S3 based on provided
    parameters. Returns a pandas DataFrame.
    """
    # 1. Load CSV
    try:
        print(f"Fetching CSV from: {S3_CSV_URL}")
        resp = requests.get(S3_CSV_URL, timeout=30)
        resp.raise_for_status()
        df = pd.read_csv(StringIO(resp.text))
        print(f"Loaded {df.shape[0]} rows from CSV")
    except Exception as e:
        print("Error loading CSV from S3:", repr(e))
        # Surface a clear API error
        raise HTTPException(status_code=500, detail=f"Error loading CSV from S3: {e}")

    # 2. Apply filters
    if year is not None:
        print("Filtering by year")
        df = df[df["year"] == year]

    if country is not None:
        print("Filtering by country")
        df = df[df["country"] == country]

    if market is not None:
        print("Filtering by market")
        df = df[df["mkt_name"] == market]

    df = df.fillna("")
    print(f"{df.shape[0]} rows after filtering")
    return df


@app.get("/fetch_data")
async def fetch_data_api(
    year: Optional[int] = Query(None),
    country: Optional[str] = Query(None),
    market: Optional[str] = Query(None),
) -> List[dict[str, Any]]:
    """
    API endpoint that returns filtered records as JSON.
    """
    # Get a DataFrame (or an HTTPException if something goes wrong)
    df_filtered = fetch_data(year=year, country=country, market=market)

    if df_filtered.empty:
        # No records for given filters
        raise HTTPException(
            status_code=404,
            detail="No data found for the specified filters.",
        )

    # Return as list of dicts – FastAPI turns this into JSON
    return df_filtered.to_dict(orient="records")


if __name__ == "__main__":
    # Run the FastAPI application using uvicorn server
    uvicorn.run("app.main:app", host="0.0.0.0", port=8080, reload=True)
