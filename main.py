import sys
import asyncio

# Fix for Windows asyncio NotImplementedError - MUST BE AT THE VERY TOP
if sys.platform == 'win32':
    asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())

from fastapi import FastAPI, HTTPException, Query
from typing import List
from schemas import WeddingList, WeddingInfo
from crawler import search_wedding_urls, extract_wedding_data
import uvicorn
import logging

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

app = FastAPI(
    title="Wedding Event Crawler API",
    description="Crawls wedding websites for upcoming events in specific locations using Crawl4AI and Groq.",
    version="1.0.0"
)

@app.get("/")
async def root():
    return {"message": "Wedding Crawler API is running. Use /weddings/{location} to start crawling."}

@app.get("/weddings/{location}", response_model=WeddingList)
async def get_weddings(
    location: str, 
    years: str = Query("2026..2028", description="Year range, e.g., 2026..2028"),
    limit: int = Query(10, description="Max wedding pages to crawl")
):
    """
    Search and crawl wedding websites for a specific location.
    """
    logger.info(f"Received request for weddings in {location} for years {years}")
    try:
        # 1. Search for URLs
        urls = await search_wedding_urls(location, years)
        if not urls:
            logger.warning(f"No URLs found for location: {location}")
            return {"weddings": []}
        
        # 2. Limit URLs
        urls_to_crawl = urls[:limit * 4]
        logger.info(f"Proceeding to crawl {len(urls_to_crawl)} URLs")
        
        # 3. Extract data
        weddings = await extract_wedding_data(urls_to_crawl, location=location)
        logger.info(f"Successfully extracted {len(weddings)} weddings")
        
        return {"weddings": weddings}
    except Exception as e:
        logger.error(f"Error processing request: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8001)
