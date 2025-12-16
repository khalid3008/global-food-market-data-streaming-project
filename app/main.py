DF_CACHE = None

from typing import Optional, List, Any
from fastapi import FastAPI, Query, HTTPException
import pandas as pd
from io import StringIO
import uvicorn
import requests

app = FastAPI()

S3_CSV_URL = "https://khalid-global-food-market-data-raw.s3.us-east-2.amazonaws.com/total_data.csv"


def fetch_data(
    year: Optional[int] = None,
    country: Optional[str] = None,
    market: Optional[str] = None,
    limit: int = 5000,
) -> pd.DataFrame:
    """
    Stream-read the CSV in chunks and filter as we go.
    Returns at most `limit` rows.
    """
    try:
        chunks = pd.read_csv(S3_CSV_URL, chunksize=50000)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error opening CSV: {e}")

    results = []
    total_found = 0

    for chunk in chunks:
        # normalize columns once per chunk
        chunk.columns = [str(c).strip().lower() for c in chunk.columns]

        # column mapping (your file has country, mkt_name, year)
        country_col = "country"
        market_col = "mkt_name"
        year_col = "year"

        # apply filters
        if year is not None:
            chunk = chunk[chunk[year_col] == year]
        if country is not None:
            chunk = chunk[chunk[country_col] == country]
        if market is not None:
            chunk = chunk[chunk[market_col] == market]

        if not chunk.empty:
            results.append(chunk)
            total_found += len(chunk)
            if total_found >= limit:
                break

    if not results:
        return pd.DataFrame()

    df = pd.concat(results, ignore_index=True).head(limit)
    df = df.fillna("")
    return df

@app.get("/")
def root():
    return {"status": "ok"}

@app.get("/fetch_data")
async def fetch_data_api(
    year: Optional[int] = Query(None),
    country: Optional[str] = Query(None),
    market: Optional[str] = Query(None),
    limit: int = Query(5000, ge=1, le=5000),
):
    print(f"/fetch_data called with year={year}, country={country}, market={market}, limit={limit}")

    df_filtered = fetch_data(year=year, country=country, market=market, limit=limit)

    if df_filtered.empty:
        raise HTTPException(status_code=404, detail="No data found for the specified filters.")

    return df_filtered.to_dict(orient="records")

if __name__ == "__main__":
    uvicorn.run("app.main:app", host="0.0.0.0", port=8080)