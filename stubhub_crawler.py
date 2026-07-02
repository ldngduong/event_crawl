import json
import logging
import math
import os
import re
from html import unescape
from typing import Any, Dict, List, Literal, Optional, Tuple
from urllib.parse import parse_qs, quote, urlparse

import httpx

logger = logging.getLogger(__name__)

StubHubSource = Literal["auto", "api", "html"]

STUBHUB_BASE_URL = "https://www.stubhub.com"
STUBHUB_DEFAULT_ALGOLIA_APP_ID = "YK5F59M0JP"
STUBHUB_DEFAULT_ALGOLIA_API_KEY = "17de0686444702a9277f95b4c7194e30"
STUBHUB_INDEX_NAME = "stubhub"
STUBHUB_DEFAULT_PAGE_SIZE = 20

STUBHUB_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/126.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,application/json;q=0.8,*/*;q=0.7",
    "Accept-Language": "en-US,en;q=0.9",
}


def _clean_url(url: Optional[str]) -> Optional[str]:
    if not isinstance(url, str) or not url.strip():
        return None
    value = unescape(url.strip()).strip('"').strip("'")
    if value.startswith("//"):
        value = f"https:{value}"
    if value.startswith("/"):
        value = f"{STUBHUB_BASE_URL}{value}"
    return value


def _strip_html(value: Any) -> Optional[str]:
    if not isinstance(value, str):
        return None
    text = re.sub(r"<[^>]+>", " ", value)
    text = re.sub(r"\s+", " ", unescape(text)).strip()
    return text or None


def _is_event_url(url: Optional[str]) -> bool:
    clean = _clean_url(url)
    if not clean:
        return False
    parsed = urlparse(clean)
    return parsed.netloc.endswith("stubhub.com") and re.search(r"/event/\d+/?", parsed.path) is not None


def _event_id_from_url(url: Optional[str]) -> Optional[str]:
    clean = _clean_url(url)
    if not clean:
        return None
    match = re.search(r"/event/(\d+)", urlparse(clean).path)
    return match.group(1) if match else None


def stubhub_search_url(*, keyword: str) -> str:
    return f"{STUBHUB_BASE_URL}/secure/Search?q={quote(keyword)}"


def _keyword_from_search_url(search_url: Optional[str]) -> Optional[str]:
    if not search_url:
        return None
    values = parse_qs(urlparse(search_url).query).get("q")
    if not values:
        return None
    keyword = values[0].strip()
    return keyword or None


async def _fetch_text(client: httpx.AsyncClient, url: str) -> str:
    response = await client.get(url, headers=STUBHUB_HEADERS)
    response.raise_for_status()
    return response.text


def _load_json_line(line: str) -> Optional[Dict[str, Any]]:
    try:
        parsed = json.loads(line.strip())
        return parsed if isinstance(parsed, dict) else None
    except Exception:
        return None


def _extract_script_json(html: str, script_id: str) -> Optional[Dict[str, Any]]:
    pattern = rf'<script[^>]+id=["\']{re.escape(script_id)}["\'][^>]*>(.*?)</script>'
    match = re.search(pattern, html or "", flags=re.IGNORECASE | re.DOTALL)
    if not match:
        return None
    raw = unescape(match.group(1)).strip()
    return _load_json_line(raw)


def _page_title(html: str) -> str:
    match = re.search(r"<title[^>]*>(.*?)</title>", html or "", flags=re.IGNORECASE | re.DOTALL)
    return _strip_html(match.group(1)) if match else ""


def _page_snippet(html: str, max_length: int = 500) -> str:
    text = _strip_html(re.sub(r"<script\b[^<]*(?:(?!</script>)<[^<]*)*</script>", " ", html or "", flags=re.IGNORECASE))
    return text[:max_length]


def _extract_search_state(html: str) -> Optional[Dict[str, Any]]:
    page_content = _extract_script_json(html, "page-content")
    if page_content and isinstance(page_content.get("eventGrids"), dict):
        return page_content

    for line in (html or "").splitlines():
        if "eventGrids" not in line or "searchKeyword" not in line:
            continue
        parsed = _load_json_line(line)
        if parsed and isinstance(parsed.get("eventGrids"), dict):
            return parsed
    return None


def _extract_app_env(html: str) -> Dict[str, str]:
    for line in (html or "").splitlines():
        if "REACT_APP_ALGOLIA_SEARCH_APPLICATION_ID" not in line:
            continue
        parsed = _load_json_line(line)
        env = parsed.get("env") if parsed else None
        if isinstance(env, dict):
            return {str(key): str(value) for key, value in env.items() if value is not None}
    return {}


def _compact_stubhub_grid_event(item: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    source_url = _clean_url(item.get("url"))
    if not _is_event_url(source_url):
        return None

    event_id = item.get("eventId") or _event_id_from_url(source_url)
    name = item.get("name") or item.get("title")
    location_name = item.get("venueName")
    city = item.get("venueCity")
    region = item.get("venueStateProvince")
    country = item.get("countryName") or item.get("countryCode")
    metadata = item.get("eventMetadata") if isinstance(item.get("eventMetadata"), dict) else {}
    common = metadata.get("common") if isinstance(metadata.get("common"), dict) else {}
    start_ms = common.get("eventStartDateTime")
    end_ms = common.get("eventEndDateTime")

    def ms_to_iso(value: Any) -> Optional[str]:
        try:
            return None if value is None else __import__("datetime").datetime.fromtimestamp(int(value) / 1000).isoformat()
        except Exception:
            return None

    return {
        "@type": "Event",
        "id": str(event_id) if event_id is not None else source_url,
        "name": name,
        "title": name,
        "url": source_url,
        "source_url": source_url,
        "startDate": ms_to_iso(start_ms),
        "endDate": ms_to_iso(end_ms),
        "description": name,
        "location": {
            "name": location_name,
            "city": city,
            "region": region,
            "country": country,
            "address": {
                "@type": "PostalAddress",
                "addressLocality": city,
                "addressRegion": region,
                "addressCountry": country,
            },
        },
        "offers": {
            "availabilityState": item.get("eventAvailabilityState"),
        },
        "eventType": "StubHub event",
        "categories": ["StubHub event"],
        "stubhub": {
            "sourceKind": "search_state",
            "eventId": event_id,
            "venueId": item.get("venueId"),
            "eventState": item.get("eventState"),
            "formattedDate": item.get("formattedDate"),
            "formattedTime": item.get("formattedTime"),
            "formattedVenueLocation": item.get("formattedVenueLocation"),
            "hasActiveListings": item.get("hasActiveListings"),
            "isMultidayEvent": item.get("isMultidayEvent"),
        },
    }


def _extract_events_from_search_state(html: str) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    state = _extract_search_state(html)
    if not state:
        return [], {}

    events: List[Dict[str, Any]] = []
    event_grids = state.get("eventGrids") if isinstance(state.get("eventGrids"), dict) else {}
    for grid in event_grids.values():
        if not isinstance(grid, dict):
            continue
        for item in grid.get("items") or []:
            if isinstance(item, dict):
                compact = _compact_stubhub_grid_event(item)
                if compact:
                    events.append(compact)

    first_grid = next((grid for grid in event_grids.values() if isinstance(grid, dict)), {})
    meta = {
        "searchKeyword": state.get("searchKeyword"),
        "searchGuid": state.get("searchGuid"),
        "totalCount": first_grid.get("totalCount") if isinstance(first_grid, dict) else None,
        "pageIndex": first_grid.get("pageIndex") if isinstance(first_grid, dict) else None,
        "pageSize": first_grid.get("pageSize") if isinstance(first_grid, dict) else None,
        "remaining": first_grid.get("remaining") if isinstance(first_grid, dict) else None,
    }
    return events, meta


def _compact_stubhub_algolia_hit(hit: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    url = _clean_url(hit.get("url"))
    if not _is_event_url(url):
        return None
    base = _compact_stubhub_grid_event({**hit, "url": url})
    if not base:
        return None
    base.setdefault("stubhub", {})["sourceKind"] = "algolia"
    base["stubhub"]["objectID"] = hit.get("objectID")
    return base


async def _search_stubhub_algolia_events(
    *,
    keyword: str,
    limit: int,
    app_id: str,
    api_key: str,
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    events: List[Dict[str, Any]] = []
    failures: List[Dict[str, Any]] = []
    per_page = min(max(limit, 1), STUBHUB_DEFAULT_PAGE_SIZE)
    max_pages = max(1, math.ceil(limit / per_page))
    hosts = [
        f"https://{app_id.lower()}-dsn.algolia.net/1/indexes/*/queries",
        f"https://{app_id.lower()}.algolia.net/1/indexes/*/queries",
    ]

    headers = {
        "Content-Type": "application/json",
        "X-Algolia-Application-Id": app_id,
        "X-Algolia-API-Key": api_key,
        "User-Agent": STUBHUB_HEADERS["User-Agent"],
    }

    async with httpx.AsyncClient(timeout=30, headers=headers) as client:
        active_host: Optional[str] = None
        for page in range(max_pages):
            params = f"query={quote(keyword)}&hitsPerPage={per_page}&page={page}"
            payload = {
                "requests": [
                    {
                        "indexName": STUBHUB_INDEX_NAME,
                        "params": params,
                    }
                ]
            }
            last_error: Optional[str] = None
            response_data: Optional[Dict[str, Any]] = None
            for host in ([active_host] if active_host else hosts):
                if not host:
                    continue
                try:
                    response = await client.post(host, json=payload)
                    response.raise_for_status()
                    response_data = response.json()
                    active_host = host
                    break
                except Exception as error:
                    last_error = str(error)

            if response_data is None:
                failures.append({"source": "algolia", "page": page, "reason": last_error or "request_failed"})
                break

            result = (response_data.get("results") or [{}])[0]
            hits = result.get("hits") if isinstance(result, dict) else []
            if not hits:
                break

            for hit in hits:
                if isinstance(hit, dict):
                    compact = _compact_stubhub_algolia_hit(hit)
                    if compact:
                        events.append(compact)
                        if len(events) >= limit:
                            break

            if len(events) >= limit:
                break
            nb_pages = result.get("nbPages") if isinstance(result, dict) else None
            if isinstance(nb_pages, int) and page + 1 >= nb_pages:
                break

    return events[:limit], failures


def _extract_stubhub_json_ld_events(html: str, source_url: str) -> List[Dict[str, Any]]:
    events: List[Dict[str, Any]] = []
    for match in re.finditer(r'<script[^>]+application/ld\+json[^>]*>(.*?)</script>', html or "", re.S | re.I):
        try:
            data = json.loads(unescape(match.group(1)))
        except Exception:
            continue
        candidates = data if isinstance(data, list) else [data]
        for item in candidates:
            if not isinstance(item, dict):
                continue
            type_value = item.get("@type")
            if not (isinstance(type_value, str) and type_value.endswith("Event")):
                continue
            location = item.get("location") if isinstance(item.get("location"), dict) else {}
            address = location.get("address") if isinstance(location.get("address"), dict) else {}
            offers = item.get("offers") if isinstance(item.get("offers"), dict) else {}
            performers = item.get("performer") if isinstance(item.get("performer"), list) else []
            event_url = _clean_url(item.get("url")) or _clean_url(source_url)
            event_id = _event_id_from_url(event_url)
            name = item.get("name")
            event_type = offers.get("category") or type_value
            events.append(
                {
                    "@type": "Event",
                    "id": event_id or event_url,
                    "name": name,
                    "title": name,
                    "url": event_url,
                    "source_url": event_url,
                    "startDate": item.get("startDate") or item.get("doorTime"),
                    "endDate": item.get("endDate"),
                    "image": item.get("image"),
                    "description": _strip_html(item.get("description")),
                    "location": {
                        "name": location.get("name"),
                        "url": location.get("url"),
                        "city": address.get("addressLocality"),
                        "region": address.get("addressRegion"),
                        "country": address.get("addressCountry"),
                        "address": {
                            "@type": "PostalAddress",
                            "streetAddress": address.get("streetAddress"),
                            "addressLocality": address.get("addressLocality"),
                            "addressRegion": address.get("addressRegion"),
                            "postalCode": address.get("postalCode"),
                            "addressCountry": address.get("addressCountry"),
                        },
                    },
                    "organizer": performers[0] if performers else None,
                    "offers": offers,
                    "eventType": event_type,
                    "categories": [event_type] if event_type else [],
                    "stubhub": {
                        "sourceKind": "json_ld",
                        "eventId": event_id,
                        "eventStatus": item.get("eventStatus"),
                        "eventAttendanceMode": item.get("eventAttendanceMode"),
                        "performer": performers,
                    },
                }
            )
    return events


async def _enrich_stubhub_details(
    events: List[Dict[str, Any]],
    parse_failures: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    enriched: List[Dict[str, Any]] = []
    async with httpx.AsyncClient(timeout=45, follow_redirects=True, headers=STUBHUB_HEADERS) as client:
        for event in events:
            url = _clean_url(event.get("source_url") or event.get("url"))
            if not url:
                enriched.append(event)
                continue
            try:
                html = await _fetch_text(client, url)
                detail_events = _extract_stubhub_json_ld_events(html, url)
                enriched.append({**event, **detail_events[0]} if detail_events else event)
                if not detail_events:
                    parse_failures.append({"url": url, "reason": "no_json_ld_event_found", "source": "detail"})
            except Exception as error:
                parse_failures.append({"url": url, "reason": str(error), "source": "detail"})
                enriched.append(event)
    return enriched


async def crawl_stubhub_events_with_diagnostics(
    *,
    keyword: Optional[str] = None,
    search_url: Optional[str] = None,
    limit: int = 50,
    source: StubHubSource = "auto",
    enrich_details: bool = True,
) -> Dict[str, Any]:
    parse_failures: List[Dict[str, Any]] = []
    events: List[Dict[str, Any]] = []
    search_meta: Dict[str, Any] = {}

    resolved_keyword = (keyword or _keyword_from_search_url(search_url) or "conference").strip()
    resolved_search_url = search_url or stubhub_search_url(keyword=resolved_keyword)
    is_detail_url = _is_event_url(resolved_search_url)

    if source in ("auto", "api") and not is_detail_url:
        app_id = os.getenv("STUBHUB_ALGOLIA_APP_ID", STUBHUB_DEFAULT_ALGOLIA_APP_ID)
        api_key = os.getenv("STUBHUB_ALGOLIA_API_KEY", STUBHUB_DEFAULT_ALGOLIA_API_KEY)
        api_events, api_failures = await _search_stubhub_algolia_events(
            keyword=resolved_keyword,
            limit=limit,
            app_id=app_id,
            api_key=api_key,
        )
        events.extend(api_events)
        parse_failures.extend(api_failures)
        if source == "api":
            if enrich_details and events:
                events = await _enrich_stubhub_details(events, parse_failures)
            return {"events": events[:limit], "parse_failures": parse_failures, "search_meta": search_meta}

    if len(events) < limit and source in ("auto", "html"):
        try:
            async with httpx.AsyncClient(timeout=45, follow_redirects=True, headers=STUBHUB_HEADERS) as client:
                html = await _fetch_text(client, resolved_search_url)
            html_events, search_meta = _extract_events_from_search_state(html)
            if not html_events and _is_event_url(resolved_search_url):
                html_events = _extract_stubhub_json_ld_events(html, resolved_search_url)
            if not html_events:
                parse_failures.append(
                    {
                        "url": resolved_search_url,
                        "reason": "no_stubhub_event_payload_found",
                        "source": "html_search",
                        "title": _page_title(html),
                        "snippet": _page_snippet(html),
                        "hint": "Pass a real StubHub /secure/Search URL from the browser, or use source=api when Algolia DNS is available.",
                    }
                )
            seen = {str(event.get("source_url") or event.get("url") or event.get("id")) for event in events}
            for event in html_events:
                key = str(event.get("source_url") or event.get("url") or event.get("id"))
                if key not in seen:
                    events.append(event)
                    seen.add(key)
        except Exception as error:
            parse_failures.append({"url": resolved_search_url, "reason": str(error), "source": "html_search"})

    if enrich_details and events:
        events = await _enrich_stubhub_details(events[:limit], parse_failures)

    deduped: Dict[str, Dict[str, Any]] = {}
    for event in events:
        key = str(event.get("source_url") or event.get("url") or event.get("id") or len(deduped))
        deduped[key] = event

    return {
        "events": list(deduped.values())[:limit],
        "parse_failures": parse_failures,
        "search_meta": search_meta,
    }


async def preview_stubhub_events(
    *,
    events: List[Dict[str, Any]],
    parse_failures: Optional[List[Dict[str, Any]]] = None,
    persist: bool = False,
) -> Dict[str, Any]:
    return {
        "mode": "preview",
        "eagle_ingest_url": "",
        "eagle_endpoint_url": "",
        "crawled_count": len(events),
        "normalized_count": 0,
        "ingested_count": 0,
        "created_count": 0,
        "updated_count": 0,
        "skipped_count": 0,
        "failed_count": 1 if persist else 0,
        "events": events,
        "results": [],
        "failures": (
            [{"reason": "stubhub_persist_not_implemented", "note": "Crawler preview is ready; add Eagle /stubhub-import before persist=true."}]
            if persist
            else []
        ),
        "parse_failures": parse_failures or [],
    }
