import asyncio
import json
import logging
import os
import re
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Literal, Optional
from urllib.parse import parse_qs, urlencode, urljoin, urlparse
from generic_mapper import ingest_generic_events_to_eagle

os.environ.setdefault(
    "CRAWL4_AI_BASE_DIRECTORY",
    str(Path(__file__).resolve().parent / ".crawl4ai_data"),
)

import httpx
from bs4 import BeautifulSoup
from crawl4ai import AsyncWebCrawler, BrowserConfig, CacheMode, CrawlerRunConfig

logger = logging.getLogger(__name__)

DEFAULT_EAGLE_API_BASE_URL = "http://localhost:3001/api/v1"
DEFAULT_EAGLE_IMPORT_BATCH_SIZE = 20
DISCOVER_BASE_URL = "https://discover.events.com"
DISCOVER_EVENT_URL_PATTERN = re.compile(
    r"https?://discover\.events\.com/[^\s\"'<>]+?/e/[^\s\"'<>]+-\d+",
    re.IGNORECASE,
)
DiscoverEventsSource = Literal["auto", "api", "html"]

_cached_events: Dict[str, Dict[str, Any]] = {}


def _discover_headers(referer: Optional[str] = None) -> Dict[str, str]:
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/126.0.0.0 Safari/537.36"
        ),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
    }
    if referer:
        headers["Referer"] = referer
    return headers


def _clean_url(url: str) -> str:
    parsed = urlparse(url)
    return f"{parsed.scheme}://{parsed.netloc}{parsed.path}"


def _as_list(value: Any) -> List[Any]:
    if value is None:
        return []
    return value if isinstance(value, list) else [value]


def _json_loads_safe(value: str) -> Optional[Any]:
    try:
        return json.loads(value)
    except Exception:
        return None


def _extract_js_var_literal(html: str, name: str) -> Optional[Any]:
    match = re.search(rf"\bvar\s+{re.escape(name)}\s*=\s*", html)
    if not match:
        return None

    index = match.end()
    while index < len(html) and html[index].isspace():
        index += 1
    if index >= len(html):
        return None

    first = html[index]
    if first in ("'", '"'):
        quote = first
        index += 1
        value_chars: List[str] = []
        escaped = False
        while index < len(html):
            char = html[index]
            if escaped:
                value_chars.append(char)
                escaped = False
            elif char == "\\":
                value_chars.append(char)
                escaped = True
            elif char == quote:
                try:
                    return json.loads(f'"{"".join(value_chars)}"')
                except Exception:
                    return "".join(value_chars)
            else:
                value_chars.append(char)
            index += 1
        return None

    if first in ("[", "{"):
        close = "]" if first == "[" else "}"
        depth = 0
        in_string = False
        escaped = False
        start = index
        while index < len(html):
            char = html[index]
            if escaped:
                escaped = False
                index += 1
                continue
            if char == "\\":
                escaped = True
                index += 1
                continue
            if char == '"':
                in_string = not in_string
                index += 1
                continue
            if in_string:
                index += 1
                continue
            if char == first:
                depth += 1
            elif char == close:
                depth -= 1
                if depth == 0:
                    return _json_loads_safe(html[start : index + 1])
            index += 1
        return None

    end_match = re.search(r"[;\n]", html[index:])
    raw = html[index : index + end_match.start() if end_match else len(html)].strip()
    if raw in {"null", "undefined"}:
        return None
    if raw in {"true", "false"}:
        return raw == "true"
    try:
        return float(raw) if "." in raw else int(raw)
    except Exception:
        return raw or None


def _selected_interest_ids(list_interests: Any) -> List[str]:
    ids: List[str] = []
    if not isinstance(list_interests, list):
        return ids
    for group in list_interests:
        if not isinstance(group, dict):
            continue
        for tag in group.get("tag") or []:
            if not isinstance(tag, dict) or not tag.get("selected"):
                continue
            tag_id = _first_string(tag.get("id"))
            if tag_id and tag_id not in ids:
                ids.append(tag_id)
    return ids


def _default_radius(distances: Any) -> str:
    if isinstance(distances, dict) and distances:
        keys = list(distances.keys())
        return str(keys[1] if len(keys) > 1 else keys[0])
    return "8.05"


def _day_list_from_search(search_url: str, html: str) -> List[str]:
    parsed = urlparse(search_url)
    query_day = _first_string(*(parse_qs(parsed.query).get("day") or []))
    forced_day = _first_string(_extract_js_var_literal(html, "forced_day"))
    day = query_day or forced_day
    if day:
        return [day]
    return [f"{datetime.utcnow().year}-{datetime.utcnow().month}-{datetime.utcnow().day}"]


def _coords_from_search(search_url: str, html: str) -> tuple[str, str]:
    parsed = urlparse(search_url)
    query = parse_qs(parsed.query)
    lat = _first_string(*(query.get("lat") or []), _extract_js_var_literal(html, "forced_lat"), _extract_js_var_literal(html, "lat"))
    lng = _first_string(*(query.get("lng") or []), _extract_js_var_literal(html, "forced_lng"), _extract_js_var_literal(html, "lng"))
    return lat or "40.7127837", lng or "-74.00594130000002"


def _read_path(record: Dict[str, Any], *keys: str) -> Any:
    current: Any = record
    for key in keys:
        if not isinstance(current, dict):
            return None
        current = current.get(key)
    return current


def _first_string(*values: Any) -> Optional[str]:
    for value in values:
        if isinstance(value, str) and value.strip():
            return value.strip()
        if isinstance(value, (int, float)):
            return str(value)
    return None


def _is_event_type(value: Any) -> bool:
    if isinstance(value, list):
        return any(_is_event_type(item) for item in value)
    return str(value).lower() == "event"


def _iter_json_ld_events(payload: Any) -> List[Dict[str, Any]]:
    events: List[Dict[str, Any]] = []
    for item in _as_list(payload):
        if not isinstance(item, dict):
            continue
        if _is_event_type(item.get("@type")):
            events.append(item)
            continue
        graph = item.get("@graph")
        if isinstance(graph, list):
            events.extend(
                graph_item
                for graph_item in graph
                if isinstance(graph_item, dict) and _is_event_type(graph_item.get("@type"))
            )
        item_list = item.get("itemListElement")
        if isinstance(item_list, list):
            for wrapper in item_list:
                event = wrapper.get("item") if isinstance(wrapper, dict) else None
                if isinstance(event, dict) and _is_event_type(event.get("@type")):
                    events.append(event)
    return events


def _extract_json_ld_events(html: str, page_url: str) -> List[Dict[str, Any]]:
    soup = BeautifulSoup(html, "html.parser")
    events: List[Dict[str, Any]] = []
    for script in soup.find_all("script", {"type": "application/ld+json"}):
        payload = _json_loads_safe(script.get_text(strip=True))
        if payload is None:
            continue
        for event in _iter_json_ld_events(payload):
            compact = _compact_discover_event(event, page_url, extraction_source="json_ld")
            if compact:
                events.append(compact)
    return events


def _extract_evt_object(html: str, page_url: str) -> Optional[Dict[str, Any]]:
    match = re.search(r'"evt"\s*:\s*\{', html)
    if not match:
        return None

    start = html.find("{", match.start())
    depth = 0
    in_string = False
    escaped = False
    end = -1
    for index in range(start, len(html)):
        char = html[index]
        if escaped:
            escaped = False
            continue
        if char == "\\":
            escaped = True
            continue
        if char == '"':
            in_string = not in_string
            continue
        if in_string:
            continue
        if char == "{":
            depth += 1
        elif char == "}":
            depth -= 1
            if depth == 0:
                end = index + 1
                break

    if end <= start:
        return None

    payload = _json_loads_safe(html[start:end])
    if not isinstance(payload, dict):
        return None
    return _compact_discover_evt(payload, page_url)


def _event_id_from_url(url: str) -> Optional[str]:
    match = re.search(r"-(\d+)$", urlparse(url).path)
    if match:
        return match.group(1)
    path = urlparse(url).path.strip("/")
    return path or None


def _compact_address(address: Any) -> Dict[str, Any]:
    if not isinstance(address, dict):
        return {}
    return {
        "@type": "PostalAddress",
        "streetAddress": _first_string(address.get("streetAddress"), address.get("street")),
        "addressLocality": _first_string(address.get("addressLocality"), address.get("city")),
        "addressRegion": _first_string(address.get("addressRegion"), address.get("state")),
        "postalCode": _first_string(address.get("postalCode"), address.get("zipcode")),
        "addressCountry": _first_string(address.get("addressCountry"), address.get("nation")),
    }


def _compact_discover_event(
    event: Dict[str, Any],
    page_url: str,
    *,
    extraction_source: str,
) -> Optional[Dict[str, Any]]:
    name = _first_string(event.get("name"), event.get("title"))
    source_url = _clean_url(_first_string(event.get("url"), page_url) or page_url)
    if not name or "/e/" not in source_url:
        return None

    location = event.get("location") if isinstance(event.get("location"), dict) else {}
    geo = location.get("geo") if isinstance(location.get("geo"), dict) else {}
    address = _compact_address(location.get("address"))
    organizer = event.get("organizer") if isinstance(event.get("organizer"), dict) else {}
    offers = event.get("offers") if isinstance(event.get("offers"), dict) else {}

    compact = {
        "@type": "Event",
        "id": _event_id_from_url(source_url),
        "name": name,
        "url": source_url,
        "source_url": source_url,
        "startDate": _first_string(event.get("startDate")),
        "endDate": _first_string(event.get("endDate")),
        "eventStatus": _first_string(event.get("eventStatus")),
        "eventAttendanceMode": _first_string(event.get("eventAttendanceMode")),
        "image": _first_string(event.get("image")),
        "description": _first_string(event.get("description")),
        "location": {
            "@type": "Place",
            "name": _first_string(location.get("name"), event.get("venueName")),
            "address": address,
            "geo": {
                "@type": "GeoCoordinates",
                "latitude": _first_string(geo.get("latitude"), event.get("latitude")),
                "longitude": _first_string(geo.get("longitude"), event.get("longitude")),
            },
        },
        "organizer": {
            "@type": "Organization",
            "name": _first_string(organizer.get("name"), event.get("organizerName")),
        },
        "offers": {
            "@type": _first_string(offers.get("@type"), "Offer"),
            "url": _first_string(offers.get("url"), event.get("ticket"), event.get("site")),
            "price": offers.get("price"),
            "priceCurrency": offers.get("priceCurrency"),
        },
        "city": address.get("addressLocality"),
        "country": address.get("addressCountry"),
        "latitude": _first_string(geo.get("latitude"), event.get("latitude")),
        "longitude": _first_string(geo.get("longitude"), event.get("longitude")),
        "discoverEvents": {
            "event_id": _event_id_from_url(source_url),
            "source": extraction_source,
        },
        "extraction_source": extraction_source,
    }

    return {key: value for key, value in compact.items() if value not in (None, "", [], {})}


def _compact_discover_evt(evt: Dict[str, Any], page_url: str) -> Optional[Dict[str, Any]]:
    source_url = _clean_url(_first_string(evt.get("url"), page_url) or page_url)
    name = _first_string(evt.get("name"))
    if not name:
        return None

    address = {
        "@type": "PostalAddress",
        "streetAddress": _first_string(evt.get("street")),
        "addressLocality": _first_string(evt.get("city")),
        "addressRegion": _first_string(evt.get("state")),
        "postalCode": _first_string(evt.get("zipcode")),
        "addressCountry": _first_string(evt.get("nation")),
    }
    compact = {
        "@type": "Event",
        "id": _first_string(evt.get("id")) or _event_id_from_url(source_url),
        "name": name,
        "url": source_url,
        "source_url": source_url,
        "startDate": _first_string(evt.get("calStart"), evt.get("startDateTime"), evt.get("startDate")),
        "endDate": _first_string(evt.get("calEnd"), evt.get("finishDateTime")),
        "timezone": _first_string(evt.get("timezone")),
        "eventStatus": "https://schema.org/EventScheduled",
        "image": _first_string(evt.get("pic")),
        "description": _first_string(evt.get("descriptionPlain"), evt.get("description")),
        "location": {
            "@type": "Place",
            "name": _first_string(evt.get("location")),
            "address": address,
            "geo": {
                "@type": "GeoCoordinates",
                "latitude": _first_string(evt.get("latitude")),
                "longitude": _first_string(evt.get("longitude")),
            },
        },
        "offers": {
            "@type": "Offer",
            "url": _first_string(evt.get("ticket"), evt.get("site")),
        },
        "city": _first_string(evt.get("city")),
        "country": _first_string(evt.get("nation")),
        "category": _first_string(evt.get("category")),
        "eventType": _first_string(evt.get("category")),
        "latitude": _first_string(evt.get("latitude")),
        "longitude": _first_string(evt.get("longitude")),
        "discoverEvents": {
            "event_id": _first_string(evt.get("id")),
            "category_id": evt.get("categoryId"),
            "page_id": evt.get("pageId"),
            "place_id": evt.get("placeId"),
            "source_type": evt.get("sourceType"),
            "origin_from": evt.get("originFrom"),
            "source": "evt_object",
        },
        "extraction_source": "evt_object",
    }

    return {key: value for key, value in compact.items() if value not in (None, "", [], {})}


def _extract_discover_event_links(html: str) -> List[str]:
    urls: List[str] = []
    for match in DISCOVER_EVENT_URL_PATTERN.finditer(html):
        clean = _clean_url(match.group(0))
        if clean not in urls:
            urls.append(clean)

    soup = BeautifulSoup(html, "html.parser")
    for anchor in soup.find_all("a", href=True):
        href = urljoin(DISCOVER_BASE_URL, str(anchor["href"]))
        clean = _clean_url(href)
        if DISCOVER_EVENT_URL_PATTERN.match(clean) and clean not in urls:
            urls.append(clean)

    return urls


def discover_search_url(*, lat: float, lng: float, day: Optional[str]) -> str:
    params = {
        "lat": lat,
        "lng": lng,
    }
    if day:
        params["day"] = day
    return f"{DISCOVER_BASE_URL}/forme?{urlencode(params)}"


def _search_url_from_request(search_url: Optional[str], lat: float, lng: float, day: Optional[str]) -> str:
    if search_url:
        return search_url
    return discover_search_url(lat=lat, lng=lng, day=day)


async def _search_discover_api_events(
    *,
    search_url: str,
    limit: int,
    radius: Optional[float],
) -> tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    events: List[Dict[str, Any]] = []
    failures: List[Dict[str, Any]] = []
    endpoint = f"{DISCOVER_BASE_URL}/datas/forme/forme.php"

    try:
        async with httpx.AsyncClient(timeout=45, follow_redirects=True) as client:
            page_response = await client.get(search_url, headers=_discover_headers(search_url))
            page_response.raise_for_status()
            html = page_response.text

            token = _first_string(_extract_js_var_literal(html, "requestToken"))
            if not token:
                return [], [
                    {
                        "source": "api",
                        "reason": "discover_events_request_token_not_found",
                        "search_url": search_url,
                    }
                ]

            lat, lng = _coords_from_search(search_url, html)
            days = _day_list_from_search(search_url, html)
            interests = _selected_interest_ids(_extract_js_var_literal(html, "listInterests"))
            selected_radius = str(radius) if radius is not None else _default_radius(_extract_js_var_literal(html, "distances"))
            page_size = min(20, max(15, limit))
            offset = 0

            while len(events) < limit:
                form = {
                    "lat": lat,
                    "lng": lng,
                    "browserLat": lat,
                    "browserLng": lng,
                    "radius": selected_radius,
                    "day": json.dumps(days),
                    "limit": str(offset),
                    "send": str(page_size),
                    "interests": ",".join(interests),
                    "showVirtual": "include",
                    "token": token,
                }
                response = await client.post(
                    endpoint,
                    data=form,
                    headers={
                        **_discover_headers(search_url),
                        "Accept": "application/json, text/plain, */*",
                        "Content-Type": "application/x-www-form-urlencoded;charset=UTF-8",
                    },
                )
                response.raise_for_status()
                payload = response.json()
                rows = payload.get("data") if isinstance(payload, dict) else None
                if not isinstance(rows, list) or not rows:
                    break

                for row in rows:
                    if not isinstance(row, dict):
                        continue
                    compact = _compact_discover_evt(row, search_url)
                    if not compact:
                        continue
                    source_url = _first_string(compact.get("url"), compact.get("source_url"))
                    if source_url:
                        key = _clean_url(source_url)
                        if key in _cached_events:
                            continue
                        _cached_events[key] = compact
                    events.append(compact)
                    if len(events) >= limit:
                        break

                offset += len(rows)
    except Exception as error:
        failures.append(
            {
                "source": "api",
                "reason": str(error),
                "search_url": search_url,
                "endpoint": endpoint,
            }
        )

    return events[:limit], failures


async def search_discover_event_urls(
    *,
    search_url: Optional[str],
    lat: float,
    lng: float,
    day: Optional[str],
    radius: Optional[float],
    limit: int,
    source: DiscoverEventsSource,
) -> Dict[str, Any]:
    url = _search_url_from_request(search_url, lat, lng, day)
    parse_failures: List[Dict[str, Any]] = []
    urls: List[str] = []

    if source in ("auto", "api"):
        api_events, api_failures = await _search_discover_api_events(search_url=url, limit=limit, radius=radius)
        parse_failures.extend(api_failures)
        for event in api_events:
            source_url = _first_string(event.get("url"), event.get("source_url"))
            if source_url:
                clean = _clean_url(source_url)
                _cached_events[clean] = event
                urls.append(clean)
        if source == "api":
            return {"urls": urls[:limit], "parse_failures": parse_failures}

    try:
        async with httpx.AsyncClient(timeout=30, follow_redirects=True) as client:
            response = await client.get(url, headers=_discover_headers())
            response.raise_for_status()
            urls.extend(link for link in _extract_discover_event_links(response.text) if link not in urls)
    except Exception as error:
        parse_failures.append({"source": "static_html_search", "url": url, "reason": str(error)})

    if len(urls) >= limit:
        return {"urls": urls[:limit], "parse_failures": parse_failures}

    if source in ("auto", "html"):
        browser_config = BrowserConfig(
            headless=True,
            verbose=False,
            viewport_width=1440,
            viewport_height=1200,
        )
        scroll_steps = max(4, min(40, (limit // 8) + 4))
        run_config = CrawlerRunConfig(
            cache_mode=CacheMode.BYPASS,
            wait_until="networkidle",
            page_timeout=90000,
            delay_before_return_html=1.5,
            scan_full_page=True,
            scroll_delay=0.7,
            max_scroll_steps=scroll_steps,
            remove_overlay_elements=True,
            simulate_user=True,
            magic=True,
        )
        try:
            async with AsyncWebCrawler(config=browser_config) as crawler:
                result = await crawler.arun(url=url, config=run_config)
                if not result.success:
                    parse_failures.append(
                        {
                            "source": "browser_infinite_scroll",
                            "url": url,
                            "reason": result.error_message,
                        }
                    )
                else:
                    for clean in _extract_discover_event_links(result.html or ""):
                        if clean not in urls:
                            urls.append(clean)
                            if len(urls) >= limit:
                                break
        except Exception as error:
            parse_failures.append({"source": "browser_infinite_scroll", "url": url, "reason": str(error)})

    return {"urls": urls[:limit], "parse_failures": parse_failures}


async def _fetch_discover_detail_event(client: httpx.AsyncClient, url: str) -> tuple[Optional[Dict[str, Any]], Optional[Dict[str, Any]]]:
    try:
        response = await client.get(url, headers=_discover_headers(url))
        response.raise_for_status()
    except Exception as error:
        return None, {"url": url, "source": "detail_fetch", "reason": str(error)}

    html = response.text
    json_ld_events = _extract_json_ld_events(html, url)
    evt_object = _extract_evt_object(html, url)

    if evt_object and json_ld_events:
        merged = {**json_ld_events[0], **evt_object}
        merged["discoverEvents"] = {
            **(json_ld_events[0].get("discoverEvents") or {}),
            **(evt_object.get("discoverEvents") or {}),
        }
        return merged, None
    if evt_object:
        return evt_object, None
    if json_ld_events:
        return json_ld_events[0], None

    title = None
    snippet = ""
    try:
        soup = BeautifulSoup(html, "html.parser")
        title = soup.title.get_text(strip=True) if soup.title else None
        snippet = soup.get_text(" ", strip=True)[:500]
    except Exception:
        pass

    return None, {
        "url": url,
        "source": "detail_parse",
        "reason": "no_discover_event_payload_found",
        "title": title,
        "snippet": snippet,
    }


async def crawl_discover_events_with_diagnostics(
    *,
    search_url: Optional[str],
    lat: float,
    lng: float,
    day: Optional[str],
    radius: Optional[float] = None,
    limit: int = 50,
    source: DiscoverEventsSource = "auto",
    enrich_details: bool = True,
    concurrency: int = 4,
) -> Dict[str, Any]:
    search_result = await search_discover_event_urls(
        search_url=search_url,
        lat=lat,
        lng=lng,
        day=day,
        radius=radius,
        limit=limit,
        source=source,
    )
    urls: List[str] = search_result["urls"]
    parse_failures: List[Dict[str, Any]] = search_result["parse_failures"]

    if not urls:
        return {
            "events": [],
            "parse_failures": [
                *parse_failures,
                {
                    "reason": "no_discover_event_urls_found",
                    "search_url": _search_url_from_request(search_url, lat, lng, day),
                    "source": source,
                },
            ],
        }

    if not enrich_details:
        events = []
        for url in urls:
            clean_url = _clean_url(url)
            events.append(
                _cached_events.get(clean_url)
                or {
                    "@type": "Event",
                    "id": _event_id_from_url(url),
                    "url": url,
                    "source_url": url,
                    "name": urlparse(url).path.strip("/").split("/")[-1],
                    "discoverEvents": {"event_id": _event_id_from_url(url), "source": "search_url_only"},
                }
            )
        return {"events": events, "parse_failures": parse_failures}

    semaphore = asyncio.Semaphore(max(1, concurrency))
    events: List[Dict[str, Any]] = []

    async with httpx.AsyncClient(timeout=45, follow_redirects=True) as client:
        async def crawl_one(url: str) -> None:
            clean_url = _clean_url(url)
            cached = _cached_events.get(clean_url)
            async with semaphore:
                event, failure = await _fetch_discover_detail_event(client, clean_url)
                if event:
                    events.append({**cached, **event} if cached else event)
                elif cached:
                    events.append(cached)
                elif failure:
                    parse_failures.append(failure)

        await asyncio.gather(*(crawl_one(url) for url in urls))

    deduped: Dict[str, Dict[str, Any]] = {}
    for event in events:
        key = str(event.get("source_url") or event.get("url") or event.get("id") or len(deduped))
        deduped[key] = event

    return {
        "events": list(deduped.values())[:limit],
        "parse_failures": parse_failures,
    }


async def ingest_discover_events_to_eagle(
    *,
    organization_id: Optional[str] = None,
    workspace_id: Optional[str] = None,
    events: List[Dict[str, Any]],
    parse_failures: Optional[List[Dict[str, Any]]] = None,
    persist: bool = False,
) -> Dict[str, Any]:
    return await ingest_generic_events_to_eagle(
        organization_id=organization_id,
        workspace_id=workspace_id,
        events=events,
        source_provider="discover",
        parse_failures=parse_failures,
        persist=persist,
    )
