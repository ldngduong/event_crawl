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

