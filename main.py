import sys
import asyncio
from pathlib import Path
from dotenv import load_dotenv

load_dotenv(Path(__file__).resolve().parent / ".env")

# Fix for Windows asyncio NotImplementedError - MUST BE AT THE VERY TOP
if sys.platform == 'win32':
    asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())

from fastapi import FastAPI, HTTPException, Query
from typing import Optional
from urllib.parse import parse_qs, unquote_plus, urlparse
from schemas import (
    EagleIngestResponse,
    DiscoverEventsIngestRequest,
    FirecrawlScraperIngestRequest,
    HumanitixCrawlResponse,
    HumanitixIngestRequest,
    InternationalConferenceAlertsCrawlResponse,
    InternationalConferenceAlertsIngestRequest,
    LumaIngestRequest,
    MeetupIngestRequest,
    StubHubIngestRequest,
    TenTimesCrawlResponse,
    TenTimesIngestRequest,
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
    ingest_stubhub_events_to_eagle,
)
from discover_events_crawler import (
    crawl_discover_events_with_diagnostics,
    ingest_discover_events_to_eagle,
)
from international_conference_alerts_crawler import (
    crawl_ica_events_with_diagnostics,
    ingest_ica_events_to_eagle,
)
from firecrawl_scraper import (
    crawl_firecrawl_events_with_diagnostics,
    ingest_firecrawl_events_to_eagle,
)
from ten_times_crawler import (
    crawl_ten_times_events_with_diagnostics,
    ingest_ten_times_events_to_eagle,
)
import httpx
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
            "firecrawl_scraper_ingest": "/firecrawl-scraper/events/ingest",
            "ten_times_ingest": "/ten-times/events/ingest",
            "international_conference_alerts": "/international-conference-alerts/events",
            "international_conference_alerts_ingest": "/international-conference-alerts/events/ingest",
        }
    }

@app.get("/debug/fetch-international-conference-alerts")
async def debug_fetch_international_conference_alerts(
    url: str = Query(
        "https://internationalconferencealerts.com/api/listings/conferences?topic=Engineering+and+Technology",
        description="URL to fetch with a normal Python HTTP client",
    ),
    client: str = Query("postman", pattern="^(postman|browser|minimal)$", description="Header preset to test"),
    cookie: Optional[str] = Query(None, description="Optional Cookie header copied from Postman/browser"),
    body_limit: int = Query(5000, ge=100, le=50000, description="Max response characters to return"),
):
    """
    Debug-only raw fetch for InternationalConferenceAlerts.
    This intentionally uses a plain HTTP request so we can compare Python vs Postman/browser behavior.
    """
    if client == "postman":
        headers = {
            "User-Agent": "PostmanRuntime/7.39.1",
            "Accept": "*/*",
            "Accept-Encoding": "gzip, deflate, br",
            "Connection": "keep-alive",
            "Postman-Token": "debug",
        }
    elif client == "browser":
        headers = {
            "User-Agent": (
                "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                "(KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36"
            ),
            "Accept": "application/json,text/plain,*/*",
            "Accept-Language": "en-US,en;q=0.9",
            "Referer": "https://internationalconferencealerts.com/conferences?q=tech&country=&month=",
            "Sec-Fetch-Dest": "empty",
            "Sec-Fetch-Mode": "cors",
            "Sec-Fetch-Site": "same-origin",
        }
    else:
        headers = {
            "Accept": "*/*",
        }
    if cookie:
        headers["Cookie"] = cookie
    try:
        async with httpx.AsyncClient(timeout=30, follow_redirects=True, headers=headers) as http_client:
            response = await http_client.get(url)
    except Exception as error:
        return {
            "ok": False,
            "url": url,
            "error": str(error),
        }

    body = response.text
    parsed_json = None
    try:
        parsed_json = response.json()
    except Exception:
        parsed_json = None

    return {
        "ok": 200 <= response.status_code < 300,
        "url": str(response.url),
        "client": client,
        "request_headers": {key: value for key, value in headers.items() if key.lower() != "cookie"},
        "used_cookie": bool(cookie),
        "status_code": response.status_code,
        "headers": dict(response.headers),
        "body_preview": body[:body_limit],
        "body_length": len(body),
        "json": parsed_json,
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
            organization_id=request.organization_id,
            workspace_id=request.workspace_id,
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
            organization_id=request.organization_id,
            workspace_id=request.workspace_id,
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
        return await ingest_stubhub_events_to_eagle(
            organization_id=request.organization_id,
            workspace_id=request.workspace_id,
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
    search_query = parse_qs(urlparse(request.search_url or "").query)
    effective_location = unquote_plus((search_query.get("location") or [""])[0]) or request.location
    effective_keyword = unquote_plus((search_query.get("keywords") or [""])[0]) or request.keyword
    logger.info(
        "Received request for Meetup ingest keyword='%s' request_location='%s' effective_location='%s' search_url='%s' source=%s limit=%s persist=%s",
        effective_keyword,
        request.location,
        effective_location,
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
            organization_id=request.organization_id,
            workspace_id=request.workspace_id,
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
            organization_id=request.organization_id,
            workspace_id=request.workspace_id,
            events=crawl_result["events"],
            parse_failures=crawl_result["parse_failures"],
            persist=request.persist,
        )
    except Exception as e:
        logger.error(f"Error crawling Discover Events: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/firecrawl-scraper/events/ingest", response_model=EagleIngestResponse)
async def ingest_firecrawl_scraper_events(request: FirecrawlScraperIngestRequest):
    """
    Generic Firecrawl-backed scraper.
    It scrapes exactly one list URL, extracts detail links, scrapes only those
    detail pages, then maps the result into the existing MICE scraper import.
    This intentionally avoids Firecrawl crawl/map endpoints to keep credit usage predictable.
    """
    logger.info(
        "Received Firecrawl scraper ingest list_url='%s' limit=%s persist=%s",
        request.list_url,
        request.limit,
        request.persist,
    )
    try:
        crawl_result = await crawl_firecrawl_events_with_diagnostics(
            list_url=request.list_url,
            limit=request.limit,
            event_url_regex=request.event_url_regex,
            include_url_patterns=request.include_url_patterns,
            exclude_url_patterns=request.exclude_url_patterns,
            same_domain_only=request.same_domain_only,
            enrich_details=request.enrich_details,
            detail_concurrency=request.detail_concurrency,
            wait_for_ms=request.wait_for_ms,
            timeout_ms=request.timeout_ms,
            max_age_ms=request.max_age_ms,
            firecrawl_proxy=request.firecrawl_proxy,
            location_country=request.location_country,
            location_languages=request.location_languages,
            source_provider=request.source_provider,
            event_type=request.event_type,
        )
        return await ingest_firecrawl_events_to_eagle(
            organization_id=request.organization_id,
            workspace_id=request.workspace_id,
            events=crawl_result["events"],
            parse_failures=crawl_result["parse_failures"],
            diagnostics=crawl_result.get("diagnostics", {}),
            persist=request.persist,
        )
    except Exception as e:
        logger.error(f"Error crawling Firecrawl scraper events: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/firecrawl-scraper/events/ingest")
async def firecrawl_scraper_ingest_usage():
    return {
        "message": "Use POST /firecrawl-scraper/events/ingest to scrape one list URL with Firecrawl, enrich detail pages, and optionally import into MICE.",
        "credit_note": "Credit usage is predictable: 1 scrape for list_url + up to limit detail scrapes when enrich_details=true. This endpoint does not call Firecrawl crawl/map.",
        "env": {
            "FIRECRAWL_API_KEY": "required for Firecrawl Cloud",
            "FIRECRAWL_API_URL": "optional; defaults to https://api.firecrawl.dev/v2",
            "EAGLE_FIRECRAWL_IMPORT_URL": "optional; defaults to /api/v1/scraper/events/discover-events-import",
        },
        "example_body": {
            "list_url": "https://internationalconferencealerts.com/conferences?q=tech&country=&month=",
            "limit": 10,
            "event_url_regex": "internationalconferencealerts\\.com/event-",
            "event_type": "Conference",
            "source_provider": "international_conference_alerts_firecrawl",
            "persist": False,
        },
    }


@app.get("/ten-times/events", response_model=TenTimesCrawlResponse)
async def get_ten_times_events(
    list_url: str = Query(
        "https://10times.com/newyork-us/conferences",
        description="10times list URL, e.g. https://10times.com/newyork-us/conferences",
    ),
    source: str = Query("auto", pattern="^(auto|html|brightdata)$", description="Crawl source"),
    limit: int = Query(50, ge=1, le=500, description="Max events to return"),
    pages: int = Query(1, ge=1, le=20, description="List pages to crawl"),
    enrich_details: bool = Query(True, description="Fetch detail pages"),
    cookie: Optional[str] = Query(None, description="Optional Cookie header copied from a logged-in browser session"),
):
    """
    Crawl 10times list/detail HTML.
    Direct HTML is tried first; Bright Data is used when source=auto and 10times
    returns its human-check wall.
    """
    logger.info(
        "Received 10times crawl list_url='%s' source=%s limit=%s pages=%s enrich_details=%s",
        list_url,
        source,
        limit,
        pages,
        enrich_details,
    )
    try:
        crawl_result = await crawl_ten_times_events_with_diagnostics(
            list_url=list_url,
            source=source,  # type: ignore[arg-type]
            limit=limit,
            pages=pages,
            enrich_details=enrich_details,
            cookie=cookie,
        )
        return {
            "count": len(crawl_result["events"]),
            "events": crawl_result["events"],
            "parse_failures": crawl_result["parse_failures"],
            "diagnostics": crawl_result.get("diagnostics", {}),
        }
    except Exception as e:
        logger.error(f"Error crawling 10times: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/ten-times/events/ingest", response_model=EagleIngestResponse)
async def ingest_ten_times_events(request: TenTimesIngestRequest):
    """
    Crawl 10times list/detail HTML, then optionally import into Eagle via generic importer.
    Set persist=false to preview mapped events first.
    """
    logger.info(
        "Received 10times ingest list_url='%s' source=%s limit=%s pages=%s persist=%s",
        request.list_url,
        request.source,
        request.limit,
        request.pages,
        request.persist,
    )
    try:
        crawl_result = await crawl_ten_times_events_with_diagnostics(
            list_url=request.list_url,
            source=request.source,
            limit=request.limit,
            pages=request.pages,
            enrich_details=request.enrich_details,
            cookie=request.cookie,
        )
        return await ingest_ten_times_events_to_eagle(
            organization_id=request.organization_id,
            workspace_id=request.workspace_id,
            events=crawl_result["events"],
            parse_failures=crawl_result["parse_failures"],
            diagnostics=crawl_result.get("diagnostics", {}),
            persist=request.persist,
        )
    except Exception as e:
        logger.error(f"Error ingesting 10times events: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/ten-times/events/ingest")
async def ten_times_ingest_usage():
    return {
        "message": "Use POST /ten-times/events/ingest to crawl 10times list/detail HTML and optionally import into Eagle.",
        "behavior": "source=auto fetches direct HTML first, then Bright Data when the direct response is 403/human-check.",
        "note": "If both direct and Bright Data return the 10times human-check wall, pass a Cookie header copied from a logged-in browser session.",
        "example_body": {
            "list_url": "https://10times.com/newyork-us/conferences",
            "source": "auto",
            "limit": 10,
            "pages": 2,
            "enrich_details": True,
            "persist": False,
        },
    }


@app.get("/international-conference-alerts/events", response_model=InternationalConferenceAlertsCrawlResponse)
async def get_international_conference_alerts_events(
    search_url: Optional[str] = Query(
        None,
        description="Optional full ICA search/detail URL. Example: https://internationalconferencealerts.com/conferences?q=tech&country=&month=",
    ),
    q: Optional[str] = Query(None, description="Search query"),
    country: Optional[str] = Query(None, description="Country filter"),
    month: Optional[str] = Query(None, description="Month filter, e.g. 2026-08 or 202608"),
    topic_slug: Optional[str] = Query(None, description="Topic route slug, e.g. engineering-and-technology"),
    subtopic_slug: Optional[str] = Query(None, description="Subtopic route slug, e.g. artificial-intelligence"),
    city_slug: Optional[str] = Query(None, description="City route slug, e.g. chicago"),
    page: int = Query(1, ge=1, description="Search page number"),
    source: str = Query("auto", pattern="^(auto|api|html|sitemap)$", description="Crawl source"),
    limit: int = Query(50, ge=1, le=1000, description="Max events to return"),
    enrich_details: bool = Query(True, description="Fetch detail pages"),
    lat: Optional[float] = Query(None, description="Latitude center for radius filtering"),
    lng: Optional[float] = Query(None, description="Longitude center for radius filtering"),
    radius_km: Optional[float] = Query(None, ge=0, description="Radius in kilometers"),
    geocode: bool = Query(True, description="Geocode missing venue coordinates"),
    proxy_url: Optional[str] = Query(
        None,
        description="Optional Playwright/Crawl4AI proxy URL, e.g. socks5://127.0.0.1:1080",
    ),
):
    """
    Crawl InternationalConferenceAlerts search/detail data.
    The crawler reports diagnostics when Cloudflare blocks HTML/detail fetches.
    """
    logger.info(
        "Received ICA crawl q='%s' country='%s' month='%s' topic='%s' subtopic='%s' city='%s' source=%s limit=%s radius=%s",
        q,
        country,
        month,
        topic_slug,
        subtopic_slug,
        city_slug,
        source,
        limit,
        radius_km,
    )
    try:
        crawl_result = await crawl_ica_events_with_diagnostics(
            search_url=search_url,
            q=q,
            country=country,
            month=month,
            topic_slug=topic_slug,
            subtopic_slug=subtopic_slug,
            city_slug=city_slug,
            page=page,
            limit=limit,
            source=source,  # type: ignore[arg-type]
            enrich_details=enrich_details,
            lat=lat,
            lng=lng,
            radius_km=radius_km,
            geocode=geocode,
            proxy_url=proxy_url,
        )
        return {
            "count": len(crawl_result["events"]),
            "events": crawl_result["events"],
            "parse_failures": crawl_result["parse_failures"],
            "diagnostics": crawl_result.get("diagnostics", {}),
        }
    except Exception as e:
        logger.error(f"Error crawling InternationalConferenceAlerts: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/international-conference-alerts/events/ingest", response_model=EagleIngestResponse)
async def ingest_international_conference_alerts_events(request: InternationalConferenceAlertsIngestRequest):
    """
    Crawl InternationalConferenceAlerts and optionally import into Eagle via /scraper/events/ica-import.
    """
    logger.info(
        "Received ICA ingest q='%s' country='%s' month='%s' topic='%s' subtopic='%s' city='%s' source=%s limit=%s persist=%s",
        request.q,
        request.country,
        request.month,
        request.topic_slug,
        request.subtopic_slug,
        request.city_slug,
        request.source,
        request.limit,
        request.persist,
    )
    try:
        crawl_result = await crawl_ica_events_with_diagnostics(
            search_url=request.search_url,
            q=request.q,
            country=request.country,
            month=request.month,
            topic_slug=request.topic_slug,
            subtopic_slug=request.subtopic_slug,
            city_slug=request.city_slug,
            page=request.page,
            limit=request.limit,
            source=request.source,
            enrich_details=request.enrich_details,
            lat=request.lat,
            lng=request.lng,
            radius_km=request.radius_km,
            geocode=request.geocode,
            proxy_url=request.proxy_url,
        )
        return await ingest_ica_events_to_eagle(
            organization_id=request.organization_id,
            workspace_id=request.workspace_id,
            events=crawl_result["events"],
            parse_failures=crawl_result["parse_failures"],
            diagnostics=crawl_result.get("diagnostics", {}),
            persist=request.persist,
        )
    except Exception as e:
        logger.error(f"Error crawling InternationalConferenceAlerts: {e}", exc_info=True)
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
