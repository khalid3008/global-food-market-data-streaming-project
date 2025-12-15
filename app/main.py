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
) -> pd.DataFrame:
    global DF_CACHE
    if DF_CACHE is None:
    	print(f"Loading CSV once from: {S3_CSV_URL}")
    	DF_CACHE = pd.read_csv(S3_CSV_URL)
    	DF_CACHE.columns = [str(c).strip().lower() for c in DF_CACHE.columns]

    df = DF_CACHE

    if "country" in df.columns:
        country_col = "country"
    else:
        country_col = df.columns[0]  # first column as fallback

    if "mkt_name" in df.columns:
        market_col = "mkt_name"
    else:
        # fallback to second column if present
        market_col = df.columns[1] if len(df.columns) > 1 else None

    if "year" in df.columns:
        year_col = "year"
    else:
        # fallback: try the 4th column (index 3) if exists
        year_col = df.columns[3] if len(df.columns) > 3 else None

    print(f"Resolved columns → year: {year_col}, country: {country_col}, market: {market_col}")

    # 3. Apply filters

    if year is not None:
        if not year_col:
            raise HTTPException(
                status_code=500,
                detail=f"'year' column not found. Columns: {df.columns.tolist()}",
            )
        print("Filtering by year")
        df = df[df[year_col] == year]

    if country is not None:
        if not country_col:
            raise HTTPException(
                status_code=500,
                detail=f"'country' column not found. Columns: {df.columns.tolist()}",
            )
        print("Filtering by country")
        df = df[df[country_col] == country]

    if market is not None:
        if not market_col:
            raise HTTPException(
                status_code=500,
                detail=f"'mkt_name' column not found. Columns: {df.columns.tolist()}",
            )
        print("Filtering by market")
        df = df[df[market_col] == market]

    df = df.fillna("")
    print(f"{df.shape[0]} rows after filtering")
    return df


@app.get("/fetch_data")
async def fetch_data_api(
    year: Optional[int] = Query(None),
    country: Optional[str] = Query(None),
    market: Optional[str] = Query(None),
) -> List[dict]:
    df_filtered = fetch_data(year=year, country=country, market=market)

    if df_filtered.empty:
        raise HTTPException(
            status_code=404,
            detail="No data found for the specified filters.",
        )

    return df_filtered.to_dict(orient="records")


if __name__ == "__main__":
    uvicorn.run("app.main:app", host="0.0.0.0", port=8080, reload=True)
