from pydantic import BaseModel, Field
from typing import List, Literal, Optional

class WeddingInfo(BaseModel):
    couple_names: Optional[str] = Field(None, description="Names of the couple getting married")
    date: Optional[str] = Field(None, description="Date of the wedding")
    time: Optional[str] = Field(None, description="Time of the wedding")
    venue: Optional[str] = Field(None, description="Name and address of the wedding venue")
    location: Optional[str] = Field(None, description="City or region where the wedding takes place")
    source_url: str = Field(..., description="URL where the information was found")

class WeddingList(BaseModel):
    weddings: List[WeddingInfo]

class HumanitixIngestRequest(BaseModel):
    organization_id: Optional[str] = Field(None, description="Optional Eagle organization UUID override")
    workspace_id: Optional[str] = Field(None, description="Optional Eagle workspace UUID, reserved for future workspace visibility")
    location: str = Field(..., description="Humanitix location slug, e.g. au--nsw--sydney or us--ny--new-york")
    keyword: str = Field("conference", description="Humanitix keyword")
    source: Literal["auto", "api", "html"] = Field(
        "auto",
        description="Humanitix crawl source: auto tries API then HTML fallback; api uses /api/search; html uses page HTML only",
    )
    limit: int = Field(50, ge=1, le=1000, description="Maximum Humanitix events to fetch and ingest")
    persist: bool = Field(True, description="When false, only crawl and return events without writing Eagle DB")

class HumanitixCrawlResponse(BaseModel):
    count: int
    events: List[dict]
    parse_failures: List[dict] = Field(default_factory=list)

class UniverseIngestRequest(BaseModel):
    organization_id: Optional[str] = Field(None, description="Optional Eagle organization UUID override")
    workspace_id: Optional[str] = Field(None, description="Optional Eagle workspace UUID")
    location: str = Field("New York, NY, USA", description="Universe location label, e.g. New York, NY, USA")
    ll: Optional[str] = Field(
        "40.7127753,-74.0059728",
        description="Latitude/longitude pair used by Universe search, e.g. 40.7127753,-74.0059728",
    )
    keyword: str = Field("music", description="Universe search keyword")
    source: Literal["auto", "api", "html"] = Field(
        "auto",
        description="Universe crawl source: auto/api uses Universe GraphQL; html reports fallback diagnostics because public pages are SPA shells",
    )
    limit: int = Field(50, ge=1, le=1000, description="Maximum Universe events to fetch")
    enrich_details: bool = Field(True, description="Fetch detail GraphQL for each event to fill description, venue, coordinates, and organizer")
    persist: bool = Field(False, description="When false, only crawl and return events without writing Eagle DB")

class LumaIngestRequest(BaseModel):
    organization_id: Optional[str] = Field(None, description="Optional Eagle organization UUID override")
    workspace_id: Optional[str] = Field(None, description="Optional Eagle workspace UUID")
    category_slug: Optional[str] = Field(
        "tech",
        description="Luma discovery category slug, e.g. tech, food. Used when calendar_api_id is omitted.",
    )
    calendar_api_id: Optional[str] = Field(
        None,
        description="Optional Luma calendar API ID. When provided, crawler fetches this calendar directly and skips category calendars.",
    )
    after: Optional[str] = Field(
        None,
        description="Optional ISO datetime lower bound for /calendar/get-items, e.g. 2026-06-03T00:00:00.000-07:00",
    )
    before: Optional[str] = Field(
        None,
        description="Optional ISO datetime upper bound for /calendar/get-items, e.g. 2026-06-04T00:00:00.000-07:00",
    )
    period: str = Field(
        "future",
        description="Luma calendar period query param. Use future or specific when after/before are provided.",
    )
    pagination_limit: int = Field(20, ge=1, le=100, description="Luma per-calendar pagination_limit")
    max_calendars: int = Field(10, ge=1, le=100, description="Maximum category calendars to scan")
    source: Literal["auto", "api", "html"] = Field(
        "auto",
        description="Luma crawl source: auto/api uses public Luma APIs; html returns diagnostics because API is sufficient.",
    )
    limit: int = Field(50, ge=1, le=1000, description="Maximum Luma events to fetch")
    persist: bool = Field(False, description="When false, only crawl and return events without writing Eagle DB")

class MeetupIngestRequest(BaseModel):
    organization_id: Optional[str] = Field(None, description="Optional Eagle organization UUID override")
    workspace_id: Optional[str] = Field(None, description="Optional Eagle workspace UUID")
    search_url: Optional[str] = Field(
        None,
        description="Optional full Meetup /find URL. Example: https://www.meetup.com/find/?location=us--ny--New+York&source=EVENTS&keywords=conference",
    )
    location: str = Field(
        "us--ny--New York",
        description="Meetup location slug from the /find URL, e.g. us--ny--New York",
    )
    keyword: str = Field("conference", description="Meetup search keyword")
    lat: Optional[float] = Field(None, description="Latitude override for Meetup GraphQL search")
    lon: Optional[float] = Field(None, description="Longitude override for Meetup GraphQL search")
    radius: Optional[float] = Field(None, description="Meetup search radius override")
    source: Literal["auto", "api", "html"] = Field(
        "auto",
        description="Meetup crawl source: auto/api uses Meetup GraphQL; html reads JSON-LD from a detail URL only",
    )
    limit: int = Field(50, ge=1, le=1000, description="Maximum Meetup events to fetch")
    enrich_details: bool = Field(True, description="Fetch event pages and parse schema.org JSON-LD for stable detail fields")
    persist: bool = Field(False, description="When false, only crawl and return events without writing Eagle DB")

class DiscoverEventsIngestRequest(BaseModel):
    organization_id: Optional[str] = Field(None, description="Optional Eagle organization UUID override")
    workspace_id: Optional[str] = Field(None, description="Optional Eagle workspace UUID")
    search_url: Optional[str] = Field(
        None,
        description="Optional full Discover Events /forme URL. Example: https://discover.events.com/forme?lat=40.7127837&lng=-74.00594130000002&day=2026-7-3",
    )
    lat: float = Field(40.7127837, description="Latitude used by Discover Events /forme search")
    lng: float = Field(-74.00594130000002, description="Longitude used by Discover Events /forme search")
    day: Optional[str] = Field("2026-7-3", description="Optional Discover Events day query, e.g. 2026-7-3")
    radius: Optional[float] = Field(
        None,
        description="Discover Events radius in kilometers. Common values: 4.83=3mi, 8.05=5mi, 24.14=15mi, 48.28=30mi, 96.56=60mi",
    )
    source: Literal["auto", "api", "html"] = Field(
        "auto",
        description="Discover Events crawl source: auto/api uses the Discover forme endpoint; html uses rendered infinite-scroll fallback.",
    )
    limit: int = Field(50, ge=1, le=1000, description="Maximum Discover Events to fetch")
    enrich_details: bool = Field(True, description="Fetch event detail pages and parse schema.org JSON-LD plus Discover evt payload")
    persist: bool = Field(False, description="When false, only crawl and return events without writing Eagle DB")

class InternationalConferenceAlertsIngestRequest(BaseModel):
    organization_id: Optional[str] = Field(None, description="Optional Eagle organization UUID")
    workspace_id: Optional[str] = Field(None, description="Optional Eagle workspace UUID")
    search_url: Optional[str] = Field(
        None,
        description="Optional full InternationalConferenceAlerts search/detail URL. Example: https://internationalconferencealerts.com/conferences?q=tech&country=&month=",
    )
    q: Optional[str] = Field(None, description="Free-text search query from the ICA conferences page")
    country: Optional[str] = Field(None, description="Optional country filter, matching ICA query parameter when available")
    month: Optional[str] = Field(None, description="Optional month filter, e.g. 2026-08 or 202608")
    topic_slug: Optional[str] = Field(None, description="Optional topic route slug, e.g. engineering-and-technology")
    subtopic_slug: Optional[str] = Field(None, description="Optional subtopic route slug, e.g. artificial-intelligence")
    city_slug: Optional[str] = Field(None, description="Optional city route slug, e.g. chicago")
    page: int = Field(1, ge=1, description="Search page number")
    source: Literal["auto", "api", "html", "sitemap"] = Field(
        "auto",
        description="auto probes possible JSON APIs, then HTML/browser, then sitemap. sitemap uses public sitemap URLs only.",
    )
    limit: int = Field(50, ge=1, le=1000, description="Maximum ICA events to return")
    enrich_details: bool = Field(True, description="Fetch each event detail page and parse JSON-LD/meta fields")
    lat: Optional[float] = Field(None, description="Latitude center for radius filtering")
    lng: Optional[float] = Field(None, description="Longitude center for radius filtering")
    radius_km: Optional[float] = Field(None, ge=0, description="Radius in kilometers. Requires lat/lng.")
    geocode: bool = Field(True, description="Geocode event venue/address when the page does not expose coordinates")
    proxy_url: Optional[str] = Field(
        None,
        description="Optional Playwright/Crawl4AI proxy URL, e.g. socks5://127.0.0.1:1080. Falls back to CRAWLER_PROXY_URL.",
    )
    persist: bool = Field(False, description="When false, only crawl and return events. When true, post to Eagle /scraper/events/ica-import")

class InternationalConferenceAlertsCrawlResponse(BaseModel):
    count: int
    events: List[dict]
    parse_failures: List[dict] = Field(default_factory=list)
    diagnostics: dict = Field(default_factory=dict)

class FirecrawlScraperIngestRequest(BaseModel):
    organization_id: Optional[str] = Field(None, description="Optional Eagle organization UUID")
    workspace_id: Optional[str] = Field(None, description="Optional Eagle workspace UUID")
    list_url: str = Field(..., description="List/search page URL to scrape with Firecrawl")
    limit: int = Field(20, ge=1, le=100, description="Maximum detail event URLs to scrape")
    event_url_regex: Optional[str] = Field(
        None,
        description="Optional regex used to pick event detail URLs from the list page. Defaults to same-domain links.",
    )
    include_url_patterns: List[str] = Field(
        default_factory=list,
        description="Optional substrings that detail URLs must contain. Empty means no include filter.",
    )
    exclude_url_patterns: List[str] = Field(
        default_factory=list,
        description="Optional substrings that detail URLs must not contain.",
    )
    same_domain_only: bool = Field(True, description="When true, keep only detail URLs on the same hostname as list_url")
    enrich_details: bool = Field(True, description="Scrape event detail pages after extracting links from list_url")
    detail_concurrency: int = Field(2, ge=1, le=5, description="Maximum concurrent Firecrawl detail scrape calls")
    wait_for_ms: int = Field(4000, ge=0, le=30000, description="Firecrawl waitFor value for rendered pages")
    timeout_ms: int = Field(60000, ge=10000, le=180000, description="Firecrawl scrape timeout")
    max_age_ms: int = Field(86_400_000, ge=0, description="Firecrawl cache maxAge; 0 disables cache reuse")
    firecrawl_proxy: Optional[str] = Field("auto", description="Firecrawl proxy option. Use null to omit.")
    location_country: Optional[str] = Field("US", description="Firecrawl location.country option")
    location_languages: List[str] = Field(default_factory=lambda: ["en-US"], description="Firecrawl location.languages option")
    source_provider: str = Field("firecrawl", description="Source provider stored in metadata")
    event_type: str = Field("Conference", description="Fallback event type when page does not expose one")
    persist: bool = Field(False, description="When false, only crawl and return events without writing Eagle DB")

class StubHubIngestRequest(BaseModel):
    organization_id: Optional[str] = Field(None, description="Optional Eagle organization UUID override")
    workspace_id: Optional[str] = Field(None, description="Optional Eagle workspace UUID")
    keyword: Optional[str] = Field(
        None,
        description="Optional StubHub search keyword. Used only when search_url is omitted or has no q query.",
    )
    search_url: Optional[str] = Field(
        None,
        description="Optional full StubHub /secure/Search or event detail URL. When provided, keyword is not required.",
    )
    source: Literal["auto", "api", "html"] = Field(
        "auto",
        description="StubHub crawl source: auto tries Algolia/public search API then HTML state fallback; detail enrichment uses JSON-LD.",
    )
    limit: int = Field(50, ge=1, le=1000, description="Maximum StubHub events to fetch")
    enrich_details: bool = Field(True, description="Fetch event detail pages and parse schema.org JSON-LD")
    persist: bool = Field(False, description="Reserved. StubHub DB import is not wired until backend /stubhub-import exists")

class EagleIngestResponse(BaseModel):
    mode: str = Field("preview", description="preview or persist")
    eagle_ingest_url: str
    eagle_endpoint_url: str
    crawled_count: int
    normalized_count: int
    ingested_count: int
    created_count: int = 0
    updated_count: int = 0
    skipped_count: int = 0
    failed_count: int
    events: List[dict]
    results: List[dict]
    failures: List[dict]
    parse_failures: List[dict] = Field(default_factory=list)
    diagnostics: dict = Field(default_factory=dict)
