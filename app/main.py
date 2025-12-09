from typing import Optional, List, Any

from fastapi import FastAPI, Query, HTTPException
import pandas as pd
import os
import uvicorn

app = FastAPI(
    title="Global Food Market API",
    description="Serve filtered records from the global food market dataset stored in S3.",
    version="0.1.0",
)

# Config
S3_BUCKET = os.getenv("RAW_DATA_BUCKET", "khalid-global-food-market-data-raw")
S3_KEY = os.getenv("RAW_DATA_KEY", "total_data.csv")
AWS_REGION = os.getenv("AWS_REGION", "us-east-2")

# using public HTTP URL, since the bucket was made public
CSV_URL = f"https://{S3_BUCKET}.s3.{AWS_REGION}.amazonaws.com/{S3_KEY}"


def fetch_data(
    year: Optional[int] = None,
    country: Optional[str] = None,
    market: Optional[str] = None,
) -> pd.DataFrame:
    """
    Load the CSV from S3 and apply optional filters.
    """
    # Load CSV
    df = pd.read_csv(CSV_URL)
    print(f"Loaded {df.shape[0]} rows from {CSV_URL}")

    # Apply filters
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
    API wrapper returning JSON records.
    """
    try:
        df_filtered = fetch_data(year=year, country=country, market=market)

        if df_filtered.empty:
            raise HTTPException(
                status_code=404,
                detail="No data found for the specified filters.",
            )

        # Return list of dicts – FastAPI will JSON-serialize this
        return df_filtered.to_dict(orient="records")

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


if __name__ == "__main__":
    uvicorn.run("app.main:app", host="0.0.0.0", port=8080, reload=True)
