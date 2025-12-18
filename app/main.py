from typing import Optional, Any, Dict, List

from fastapi import FastAPI, Query, HTTPException
import pandas as pd
import uvicorn

app = FastAPI()

S3_CSV_URL = "https://khalid-global-food-market-data-raw.s3.us-east-2.amazonaws.com/total_data.csv"

CHUNK_SIZE = 50_000
MAX_LIMIT = 200  # keep small to avoid App Runner timeouts


def fetch_data_paged(
    year: Optional[int] = None,
    country: Optional[str] = None,
    market: Optional[str] = None,
    offset: int = 0,
    limit: int = 200,
) -> Dict[str, Any]:
    """
    Stream-read CSV in chunks, filter, and return a page of results.
    offset/limit apply AFTER filtering.
    """
    # Safety
    if limit > MAX_LIMIT:
        limit = MAX_LIMIT

    # We need to collect up to offset+limit rows (filtered) to serve this page.
    target = offset + limit
    collected = 0
    results: List[pd.DataFrame] = []

    try:
        chunks = pd.read_csv(S3_CSV_URL, chunksize=CHUNK_SIZE)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error opening CSV: {e}")

    for chunk in chunks:
        # normalize columns
        chunk.columns = [str(c).strip().lower() for c in chunk.columns]

        # your dataset columns
        country_col = "country"
        market_col = "mkt_name"
        year_col = "year"

        # normalize filter columns (important)
        # year can be read as int/float/string depending on CSV; coerce safely
        if year_col in chunk.columns:
            chunk[year_col] = pd.to_numeric(chunk[year_col], errors="coerce").astype("Int64")

        if year is not None:
            chunk = chunk[chunk[year_col] == year]
        if country is not None:
            chunk = chunk[chunk[country_col] == country]
        if market is not None:
            chunk = chunk[chunk[market_col] == market]

        if not chunk.empty:
            results.append(chunk)
            collected += len(chunk)

            # stop early once we have enough to serve this page
            if collected >= target:
                break

    if not results:
        return {"data": [], "next_offset": None}

    df = pd.concat(results, ignore_index=True)
    df = df.fillna("")

    # If offset is beyond available filtered rows:
    if offset >= len(df):
        return {"data": [], "next_offset": None}

    page = df.iloc[offset : offset + limit]
    next_offset = offset + limit if (offset + limit) < len(df) else None

    return {
        "data": page.to_dict(orient="records"),
        "next_offset": next_offset,
    }


@app.get("/")
def root():
    return {"status": "ok"}


@app.get("/fetch_data")
async def fetch_data_api(
    year: Optional[int] = Query(None),
    country: Optional[str] = Query(None),
    market: Optional[str] = Query(None),
    offset: int = Query(0, ge=0),
    limit: int = Query(MAX_LIMIT, ge=1, le=MAX_LIMIT),
):
    return fetch_data_paged(year=year, country=country, market=market, offset=offset, limit=limit)


if __name__ == "__main__":
    uvicorn.run("app.main:app", host="0.0.0.0", port=8080)