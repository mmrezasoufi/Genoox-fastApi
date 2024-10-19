from fastapi import FastAPI
from pydantic import BaseModel
from typing import List
import httpx
import asyncio
import logging
import time

app = FastAPI()

class Variant(BaseModel):
    id: int
    chr: str
    Pos: int
    Ref: str
    Alt: str

class ResponseModel(BaseModel):
    id: int
    chrom: str
    pos: int
    ref: str
    alt: str
    classification: str | None
    db_snp: str | None
    c_dot: str | None
    transcript: str | None
    gene: str | None
    score: float | None

URL = "https://franklin.genoox.com/api/classify"

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("app.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

async def fetch_variant_data(client: httpx.AsyncClient, variant: Variant):
    data = {
        "variant": {
            "chrom": variant.chr,
            "alt": variant.Alt,
            "pos": variant.Pos,
            "ref": variant.Ref,
            "reference_version": "hg19"
        },
        "is_versioned_request": False
    }
    print(data)
    try:
        res = await client.post(URL, json=data)
        res.raise_for_status()
        result = res.json()
        return ResponseModel(
            id=variant.id,
            chrom=variant.chr,
            pos=variant.Pos,
            ref=variant.Ref,
            alt=variant.Alt,
            classification=result.get("classification"),
            db_snp=result.get("db_snp", None),
            c_dot=result.get("c_dot", None),
            transcript=result.get("transcript", None),
            gene=result.get("gene", None),
            score=result.get("score", None)
        )
    except Exception as e:
        logger.warning(f"Failed to fetch data for variant: {variant}. Error: {e}")
    return None

@app.post("/classify_variants/", response_model=List[ResponseModel])
async def classify_variants(variants: List[Variant]):
    start = time.time()
    async with httpx.AsyncClient() as client:
        tasks = [fetch_variant_data(client, variant) for variant in variants]
        results = await asyncio.gather(*tasks)
    logger.info(f"Processed {len(variants)} variants in {time.time() - start} seconds.")
    return [result for result in results if result]

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="localhost", port=2887)
