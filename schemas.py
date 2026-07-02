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

class StubHubIngestRequest(BaseModel):
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

