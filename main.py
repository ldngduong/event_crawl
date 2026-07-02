import sys
import asyncio

# Fix for Windows asyncio NotImplementedError - MUST BE AT THE VERY TOP
if sys.platform == 'win32':
    asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())

from fastapi import FastAPI, HTTPException, Query
from typing import Optional
from schemas import (
    EagleIngestResponse,
    DiscoverEventsIngestRequest,
    HumanitixCrawlResponse,
    HumanitixIngestRequest,
    LumaIngestRequest,
    MeetupIngestRequest,
    StubHubIngestRequest,
    UniverseIngestRequest,
    WeddingInfo,
    WeddingList,
)
from crawler import search_wedding_urls, extract_wedding_data
from humanitix_crawler import (
    crawl_humanitix_events_with_diagnostics,
    ingest_humanitix_events_to_eagle,
)
from universe_crawler import (
    crawl_universe_events_with_diagnostics,
    ingest_universe_events_to_eagle,
)
from luma_crawler import (
    crawl_luma_events_with_diagnostics,
    ingest_luma_events_to_eagle,
)
from meetup_crawler import (
    crawl_meetup_events_with_diagnostics,
    ingest_meetup_events_to_eagle,
)
from stubhub_crawler import (
    crawl_stubhub_events_with_diagnostics,
    preview_stubhub_events,
)
from discover_events_crawler import (
    crawl_discover_events_with_diagnostics,
    ingest_discover_events_to_eagle,
)
import uvicorn
import logging

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

app = FastAPI(
    title="Eagle Event Crawler API",
    description="Crawls public event sources and wedding websites using Crawl4AI, JSON-LD, and source-specific APIs.",
    version="1.0.0"
)

@app.get("/")
async def root():
    return {
        "message": "Crawler API is running.",
        "endpoints": {
            "weddings": "/weddings/{location}",
            "humanitix_events": "/humanitix/events/{location}",
            "humanitix_ingest": "/humanitix/events/ingest",
            "universe_ingest": "/universe/events/ingest",
            "luma_ingest": "/luma/events/ingest",
            "meetup_ingest": "/meetup/events/ingest",
            "stubhub_ingest": "/stubhub/events/ingest",
            "discover_events_ingest": "/discover/events/ingest",
        }
    }

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

@app.post("/humanitix/events/ingest", response_model=EagleIngestResponse)
async def ingest_humanitix_events(request: HumanitixIngestRequest):
    """
    Crawl Humanitix event pages, then send the crawled events to Eagle backend for direct DB import.
    Set persist=false to only return crawled events without writing database rows.
    """
    logger.info(
        "Received request for Humanitix ingest organization='%s' workspace='%s' location='%s' keyword='%s' persist=%s",
        request.organization_id,
        request.workspace_id,
        request.location,
        request.keyword,
        request.persist,
    )
    try:
        crawl_result = await crawl_humanitix_events_with_diagnostics(
            keyword=request.keyword,
            location=request.location,
            limit=request.limit,
            source=request.source,
        )
        return await ingest_humanitix_events_to_eagle(
            organization_id=request.organization_id,
            workspace_id=request.workspace_id,
            events=crawl_result["events"],
            parse_failures=crawl_result["parse_failures"],
            persist=request.persist,
        )
    except Exception as e:
        logger.error(f"Error ingesting Humanitix events: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/humanitix/events/ingest")
async def humanitix_ingest_usage():
    return {
        "message": "Use POST /humanitix/events/ingest to crawl Humanitix and import events into Eagle.",
        "example_body": {
            "organization_id": "organization-uuid",
            "workspace_id": "workspace-uuid",
            "location": "au--nsw--sydney",
            "keyword": "conference",
            "source": "auto",
            "limit": 20,
            "persist": True,
        },
        "note": "persist=false only crawls and returns events. persist=true posts the crawled events to /api/v1/scraper/events/humanitix-import and writes DB.",
    }


@app.get("/humanitix/events/{location}", response_model=HumanitixCrawlResponse)
async def get_humanitix_events(
    location: str,
    keyword: str = Query("conference", description="Search keyword, e.g. conference, meetup, expo"),
    source: str = Query("auto", pattern="^(auto|api|html)$", description="Crawl source: auto, api, or html"),
    limit: int = Query(20, ge=1, le=1000, description="Max Humanitix events to crawl"),
):
    """
    Search Humanitix event pages and crawl public JSON-LD data without using an API key.
    Prefer passing the Humanitix location slug, e.g. au--nsw--sydney or us--ny--new-york.
    """
    logger.info(
        "Received request for Humanitix crawl location='%s' keyword='%s' source=%s limit=%s",
        location,
        keyword,
        source,
        limit,
    )
    try:
        crawl_result = await crawl_humanitix_events_with_diagnostics(
            keyword=keyword,
            location=location,
            limit=limit,
            source=source,  # type: ignore[arg-type]
        )
        return {
            "count": len(crawl_result["events"]),
            "events": crawl_result["events"],
            "parse_failures": crawl_result["parse_failures"],
        }
    except Exception as e:
        logger.error(f"Error crawling Humanitix: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/universe/events/ingest", response_model=EagleIngestResponse)
async def ingest_universe_events(request: UniverseIngestRequest):
    """
    Crawl Universe events via the same GraphQL API used by universe.com.
    Set persist=false to only return crawled events without writing database rows.
    """
    logger.info(
        "Received request for Universe ingest location='%s' ll='%s' keyword='%s' source=%s limit=%s persist=%s",
        request.location,
        request.ll,
        request.keyword,
        request.source,
        request.limit,
        request.persist,
    )
    try:
        crawl_result = await crawl_universe_events_with_diagnostics(
            keyword=request.keyword,
            location=request.location,
            ll=request.ll,
            limit=request.limit,
            source=request.source,
            enrich_details=request.enrich_details,
        )
        return await ingest_universe_events_to_eagle(
            organization_id=None,
            workspace_id=None,
            events=crawl_result["events"],
            parse_failures=crawl_result["parse_failures"],
            persist=request.persist,
        )
    except Exception as e:
        logger.error(f"Error crawling Universe: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/luma/events/ingest", response_model=EagleIngestResponse)
async def ingest_luma_events(request: LumaIngestRequest):
    """
    Crawl Luma public discovery/calendar APIs, then optionally send the crawled
    events to Eagle backend for direct DB import.
    """
    logger.info(
        "Received request for Luma ingest category_slug='%s' calendar_api_id='%s' after='%s' before='%s' period=%s source=%s limit=%s persist=%s",
        request.category_slug,
        request.calendar_api_id,
        request.after,
        request.before,
        request.period,
        request.source,
        request.limit,
        request.persist,
    )
    try:
        crawl_result = await crawl_luma_events_with_diagnostics(
            category_slug=request.category_slug,
            calendar_api_id=request.calendar_api_id,
            after=request.after,
            before=request.before,
            period=request.period,
            pagination_limit=request.pagination_limit,
            max_calendars=request.max_calendars,
            limit=request.limit,
            source=request.source,
        )
        return await ingest_luma_events_to_eagle(
            events=crawl_result["events"],
            parse_failures=crawl_result["parse_failures"],
            diagnostics=crawl_result.get("diagnostics"),
            persist=request.persist,
        )
    except Exception as e:
        logger.error(f"Error crawling Luma: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/stubhub/events/ingest", response_model=EagleIngestResponse)
async def ingest_stubhub_events(request: StubHubIngestRequest):
    """
    Crawl StubHub search results. Search prefers StubHub's public Algolia-backed
    search data; detail enrichment parses schema.org JSON-LD from event pages.
    persist=true is intentionally not wired until Eagle backend adds /stubhub-import.
    """
    logger.info(
        "Received request for StubHub ingest keyword='%s' search_url='%s' source=%s limit=%s persist=%s",
        request.keyword,
        request.search_url,
        request.source,
        request.limit,
        request.persist,
    )
    try:
        crawl_result = await crawl_stubhub_events_with_diagnostics(
            keyword=request.keyword,
            search_url=request.search_url,
            limit=request.limit,
            source=request.source,
            enrich_details=request.enrich_details,
        )
        return await preview_stubhub_events(
            events=crawl_result["events"],
            parse_failures=crawl_result["parse_failures"],
            persist=request.persist,
        )
    except Exception as e:
        logger.error(f"Error crawling StubHub: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/meetup/events/ingest", response_model=EagleIngestResponse)
async def ingest_meetup_events(request: MeetupIngestRequest):
    """
    Crawl Meetup search results via Meetup's GraphQL eventSearch cursor flow.
    Detail enrichment parses schema.org JSON-LD from event pages.
    Set persist=false to only return crawled events without writing database rows.
    """
    logger.info(
        "Received request for Meetup ingest keyword='%s' location='%s' search_url='%s' source=%s limit=%s persist=%s",
        request.keyword,
        request.location,
        request.search_url,
        request.source,
        request.limit,
        request.persist,
    )
    try:
        crawl_result = await crawl_meetup_events_with_diagnostics(
            search_url=request.search_url,
            keyword=request.keyword,
            location=request.location,
            lat=request.lat,
            lon=request.lon,
            radius=request.radius,
            limit=request.limit,
            source=request.source,
            enrich_details=request.enrich_details,
        )
        return await ingest_meetup_events_to_eagle(
            events=crawl_result["events"],
            parse_failures=crawl_result["parse_failures"],
            persist=request.persist,
        )
    except Exception as e:
        logger.error(f"Error crawling Meetup: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/discover/events/ingest", response_model=EagleIngestResponse)
async def ingest_discover_events(request: DiscoverEventsIngestRequest):
    """
    Crawl Discover Events search results. Auto/html uses browser-rendered
    infinite scroll, then parses schema.org JSON-LD and the Discover evt payload
    from each detail page. Set persist=false to preview only.
    """
    logger.info(
        "Received request for Discover Events ingest search_url='%s' lat=%s lng=%s day='%s' radius=%s source=%s limit=%s persist=%s",
        request.search_url,
        request.lat,
        request.lng,
        request.day,
        request.radius,
        request.source,
        request.limit,
        request.persist,
    )
    try:
        crawl_result = await crawl_discover_events_with_diagnostics(
            search_url=request.search_url,
            lat=request.lat,
            lng=request.lng,
            day=request.day,
            radius=request.radius,
            limit=request.limit,
            source=request.source,
            enrich_details=request.enrich_details,
        )
        return await ingest_discover_events_to_eagle(
            events=crawl_result["events"],
            parse_failures=crawl_result["parse_failures"],
            persist=request.persist,
        )
    except Exception as e:
        logger.error(f"Error crawling Discover Events: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


if __name__ == "__main__":
    def proactor_loop_factory(use_subprocess: bool = False):
        if sys.platform == "win32":
            return asyncio.ProactorEventLoop()
        return asyncio.new_event_loop()

    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=8006,
        loop=proactor_loop_factory,
        reload=False,
    )



