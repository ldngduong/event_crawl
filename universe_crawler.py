import hashlib
import json
import logging
import math
import os
import re
from datetime import datetime
from html import unescape
from typing import Any, Dict, List, Literal, Optional, Tuple
from urllib.parse import quote, urlparse

from generic_mapper import ingest_generic_events_to_eagle
from schemas import GenericMappedEventDict, GenericOccurrenceDict

import httpx

logger = logging.getLogger(__name__)

UniverseSource = Literal["auto", "api", "html"]

UNIVERSE_BASE_URL = "https://www.universe.com"
UNIVERSE_DISCOVER_GRAPHQL_URL = f"{UNIVERSE_BASE_URL}/discover/graphql"
UNIVERSE_GRAPHQL_URL = f"{UNIVERSE_BASE_URL}/graphql"
UNIVERSE_DEFAULT_LIMIT_PER_PAGE = 24
DEFAULT_EAGLE_API_BASE_URL = "http://localhost:3001/api/v1"
DEFAULT_EAGLE_IMPORT_BATCH_SIZE = 10

UNIVERSE_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/126.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json",
    "Content-Type": "application/json",
    "Origin": UNIVERSE_BASE_URL,
    "Referer": f"{UNIVERSE_BASE_URL}/explore",
}

EVENT_FRAGMENT = """
fragment EventFragment on Event {
  id
  title
  slug
  currency
  minPrice
  maxPrice
  hiddenPrice
  hiddenDate
  dateDisplayOption
  geo {
    address
    city
  }
  coverPhoto {
    id
    fullUrl: url
    url(height: $height, width: $width, quality: LIGHTEST)
    webpUrl: url(height: $height, width: $width, quality: LIGHTEST, format: WEBP)
  }
  timeSlots {
    totalCount
    nodes(limit: 1) {
      startAt
      endAt
    }
  }
  host {
    id
    name
    url
  }
  source {
    id
    name
    url
  }
  type
}
"""

DISCOVER_SEARCH_EVENTS_QUERY = EVENT_FRAGMENT + """
query DiscoverSearchEvents(
  $latitude: Float,
  $longitude: Float,
  $limit: Int,
  $query: String,
  $categories: [Category],
  $time: TimeFilter,
  $freeEventsOnly: Boolean,
  $page: Int,
  $height: Int,
  $width: Int
) {
  events: search(
    latitude: $latitude,
    longitude: $longitude,
    limit: $limit
    query: $query,
    categories: $categories,
    time: $time,
    freeEventsOnly: $freeEventsOnly,
    page: $page,
    distance: 50,
  ) {
    totalCount
    events {
      ...EventFragment,
    }
  }
}
"""

CACHEABLE_EVENT_QUERY = """
query CacheableEvent($id: ID!) {
  event(id: $id) {
    id
    address
    transactionCurrency
    description(format: HTML)
    latitude
    longitude
    maxPrice
    minPrice
    region
    slug
    slugParam
    soldOut
    state
    title
    venueName
    virtual
    countryCode
    salesEnded
    category {
      id
      name
    }
    firstTimeSlot {
      id
      startAt
      endAt
      startStamp
      endStamp
      state
    }
    coverPhoto {
      fullUrl: url
      url(height: 1200)
      uploadId
    }
    user {
      id
      slug
      name
      firstName
      lastName
      businessAddress
      businessEmail
      businessPhoneNumber
    }
    tags {
      name
      slug
    }
    allImages {
      url
      fullUrl: url
    }
    timezone
    ageLimit
  }
}
"""

UNIVERSE_LOCATION_PRESETS: Dict[str, Tuple[str, float, float]] = {
    "new york": ("New York, NY, USA", 40.7127753, -74.0059728),
    "new york, ny": ("New York, NY, USA", 40.7127753, -74.0059728),
    "nyc": ("New York, NY, USA", 40.7127753, -74.0059728),
    "sydney": ("Sydney NSW, Australia", -33.8688197, 151.2092955),
    "los angeles": ("Los Angeles, CA, USA", 34.0549076, -118.242643),
    "chicago": ("Chicago, IL, USA", 41.8781136, -87.6297982),
    "san francisco": ("San Francisco, CA, USA", 37.7749295, -122.4194155),
}


def _strip_html(value: Any) -> Optional[str]:
    if not isinstance(value, str):
        return None
    text = value
    text = re.sub(r"(?i)<\s*br\s*/?\s*>", "\n", text)
    text = re.sub(r"(?i)</\s*(p|div|h[1-6]|blockquote|ul|ol)\s*>", "\n\n", text)
    text = re.sub(r"(?i)<\s*li[^>]*>", "\n- ", text)
    text = re.sub(r"(?i)</\s*li\s*>", "\n", text)
    text = re.sub(r"<[^>]+>", " ", text)
    text = unescape(text)
    lines = [re.sub(r"[ \t]+", " ", line).strip() for line in text.splitlines()]
    cleaned_lines: List[str] = []
    previous_blank = False
    for line in lines:
        if not line:
            if not previous_blank and cleaned_lines:
                cleaned_lines.append("")
            previous_blank = True
            continue
        cleaned_lines.append(line)
        previous_blank = False
    text = "\n".join(cleaned_lines).strip()
    return text or None


def _full_universe_image_url(value: Any) -> Optional[str]:
    if not isinstance(value, str) or not value.strip():
        return None
    text = value.strip()
    parsed = urlparse(text)
    if not parsed.scheme or not parsed.netloc:
        return text
    return f"{parsed.scheme}://{parsed.netloc}{parsed.path}"


def _as_float(value: Any) -> Optional[float]:
    try:
        if value is None or value == "":
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def _as_int(value: Any) -> Optional[int]:
    try:
        if value is None or value == "":
            return None
        return int(float(value))
    except (TypeError, ValueError):
        return None


def _parse_lat_lon(ll: Optional[str]) -> Tuple[Optional[float], Optional[float]]:
    if not ll:
        return None, None
    parts = [part.strip() for part in ll.split(",")]
    if len(parts) != 2:
        return None, None
    return _as_float(parts[0]), _as_float(parts[1])


def _resolve_location(location: str, ll: Optional[str]) -> Tuple[str, Optional[float], Optional[float]]:
    lat, lon = _parse_lat_lon(ll)
    if lat is not None and lon is not None:
        return location, lat, lon

    preset = UNIVERSE_LOCATION_PRESETS.get(location.strip().lower())
    if preset:
        return preset

    return location, None, None


def _event_url_from_slug_param(slug_param: Optional[str], slug: Optional[str]) -> Optional[str]:
    identifier = slug_param or slug
    if not isinstance(identifier, str) or not identifier.strip():
        return None
    return f"{UNIVERSE_BASE_URL}/events/{identifier.strip()}"


def _event_identifier_from_url(url: str) -> Optional[str]:
    path = urlparse(url).path.strip("/")
    if not path.startswith("events/"):
        return None
    return path.split("/", 1)[1].strip("/") or None


def _search_result_identifier(event: Dict[str, Any]) -> Optional[str]:
    for url_key in ("source_url", "url"):
        url_value = event.get(url_key)
        if isinstance(url_value, str):
            identifier = _event_identifier_from_url(url_value)
            if identifier:
                return identifier

    source = event.get("source") if isinstance(event.get("source"), dict) else {}
    source_url = source.get("url")
    if isinstance(source_url, str):
        identifier = _event_identifier_from_url(source_url)
        if identifier:
            return identifier
    slug = event.get("slug")
    if isinstance(slug, str) and slug:
        return slug
    event_id = event.get("id")
    return str(event_id) if event_id else None


def _graphql_url(base_url: str, payload: Dict[str, Any]) -> str:
    body = json.dumps(payload, separators=(",", ":"))
    return f"{base_url}?sha256={hashlib.sha256(body.encode()).hexdigest()}"


async def _post_graphql(client: httpx.AsyncClient, url: str, payload: Dict[str, Any]) -> Dict[str, Any]:
    body = json.dumps(payload, separators=(",", ":"))
    response = await client.post(_graphql_url(url, payload), content=body)
    response.raise_for_status()
    data = response.json()
    if data.get("errors"):
        raise RuntimeError(json.dumps(data["errors"]))
    return data.get("data") or {}


def _compact_universe_search_event(event: Dict[str, Any]) -> Dict[str, Any]:
    source = event.get("source") if isinstance(event.get("source"), dict) else {}
    host = event.get("host") if isinstance(event.get("host"), dict) else {}
    geo = event.get("geo") if isinstance(event.get("geo"), dict) else {}
    timeslot = {}
    time_slots = event.get("timeSlots") if isinstance(event.get("timeSlots"), dict) else {}
    nodes = time_slots.get("nodes") if isinstance(time_slots.get("nodes"), list) else []
    if nodes and isinstance(nodes[0], dict):
        timeslot = nodes[0]

    source_url = source.get("url") if isinstance(source.get("url"), str) else None
    title = event.get("title")
    slug = event.get("slug")
    cover_photo = event.get("coverPhoto") if isinstance(event.get("coverPhoto"), dict) else {}

    return {
        "@type": "Event",
        "id": event.get("id"),
        "name": title,
        "title": title,
        "url": source_url or _event_url_from_slug_param(None, slug if isinstance(slug, str) else None),
        "source_url": source_url,
        "slug": slug,
        "startDate": timeslot.get("startAt"),
        "endDate": timeslot.get("endAt"),
        "image": _full_universe_image_url(
            cover_photo.get("fullUrl") or cover_photo.get("url") or cover_photo.get("webpUrl")
        ),
        "description": title,
        "location": {
            "name": geo.get("city"),
            "city": geo.get("city"),
            "address": {
                "@type": "PostalAddress",
                "streetAddress": geo.get("address"),
                "addressLocality": geo.get("city"),
            },
        },
        "organizer": {
            "@type": "Organization",
            "name": host.get("name"),
            "url": host.get("url"),
        },
        "offers": {
            "price": event.get("minPrice"),
            "highPrice": event.get("maxPrice"),
            "priceCurrency": event.get("currency"),
        },
        "categories": [event.get("type")] if event.get("type") else [],
        "eventType": event.get("type"),
        "universe": {
            "sourceKind": "search",
            "slug": slug,
            "source": source,
            "host": host,
            "geo": geo,
            "timeSlots": time_slots,
        },
    }


def _compact_universe_detail_event(event: Dict[str, Any]) -> Dict[str, Any]:
    first_time_slot = event.get("firstTimeSlot") if isinstance(event.get("firstTimeSlot"), dict) else {}
    category = event.get("category") if isinstance(event.get("category"), dict) else {}
    user = event.get("user") if isinstance(event.get("user"), dict) else {}
    tags = event.get("tags") if isinstance(event.get("tags"), list) else []
    cover_photo = event.get("coverPhoto") if isinstance(event.get("coverPhoto"), dict) else {}
    all_images = event.get("allImages") if isinstance(event.get("allImages"), list) else []
    slug_param = event.get("slugParam") if isinstance(event.get("slugParam"), str) else None
    slug = event.get("slug") if isinstance(event.get("slug"), str) else None
    source_url = _event_url_from_slug_param(slug_param, slug)

    tag_names = [tag.get("name") for tag in tags if isinstance(tag, dict) and tag.get("name")]
    event_type = category.get("name") or (tag_names[0] if tag_names else None)
    location: Dict[str, Any] = {
        "name": event.get("venueName"),
        "country": event.get("countryCode"),
        "latitude": event.get("latitude"),
        "longitude": event.get("longitude"),
        "address": {
            "@type": "PostalAddress",
            "streetAddress": event.get("address"),
            "addressRegion": event.get("region"),
            "addressCountry": event.get("countryCode"),
        },
    }

    return {
        "@type": "Event",
        "id": event.get("id") or slug_param or slug,
        "name": event.get("title"),
        "title": event.get("title"),
        "url": source_url,
        "source_url": source_url,
        "startDate": first_time_slot.get("startAt"),
        "endDate": first_time_slot.get("endAt"),
        "timezone": event.get("timezone"),
        "image": _full_universe_image_url(
            cover_photo.get("fullUrl")
            or cover_photo.get("url")
            or next(
                (
                    image.get("fullUrl") or image.get("url")
                    for image in all_images
                    if isinstance(image, dict) and (image.get("fullUrl") or image.get("url"))
                ),
                None,
            )
        ),
        "description": _strip_html(event.get("description")),
        "location": location,
        "organizer": {
            "@type": "Organization",
            "name": user.get("name") or user.get("firstName"),
            "url": f"{UNIVERSE_BASE_URL}/users/{user.get('slug')}" if user.get("slug") else None,
            "email": user.get("businessEmail"),
            "phone": user.get("businessPhoneNumber"),
            "address": user.get("businessAddress"),
        },
        "offers": {
            "price": event.get("minPrice"),
            "highPrice": event.get("maxPrice"),
            "priceCurrency": event.get("transactionCurrency"),
        },
        "categories": tag_names or ([event_type] if event_type else []),
        "eventType": event_type,
        "country": event.get("countryCode"),
        "universe": {
            "source": "detail",
            "slug": slug,
            "slugParam": slug_param,
            "state": event.get("state"),
            "soldOut": event.get("soldOut"),
            "salesEnded": event.get("salesEnded"),
            "virtual": event.get("virtual"),
            "ageLimit": event.get("ageLimit"),
        },
    }


def _merge_universe_events(base: Dict[str, Any], detail: Dict[str, Any]) -> Dict[str, Any]:
    merged = {**base, **{key: value for key, value in detail.items() if value not in (None, "", [], {})}}
    for key in ("location", "organizer", "offers", "universe"):
        if isinstance(base.get(key), dict) or isinstance(detail.get(key), dict):
            merged[key] = {
                **(base.get(key) if isinstance(base.get(key), dict) else {}),
                **(detail.get(key) if isinstance(detail.get(key), dict) else {}),
            }
    return merged


def _read_record(value: Any) -> Dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _first_string(*values: Any) -> Optional[str]:
    for value in values:
        if isinstance(value, str) and value.strip():
            return value.strip()
        if isinstance(value, (int, float)) and not isinstance(value, bool):
            return str(value)
    return None


def _map_universe_event_to_generic(raw_event: Dict[str, Any]) -> Optional[GenericMappedEventDict]:
    name = _first_string(raw_event.get("name"), raw_event.get("title"))
    if not name:
        return None

    location = _read_record(raw_event.get("location"))
    address = _read_record(location.get("address"))
    organizer = _read_record(raw_event.get("organizer"))
    offers = _read_record(raw_event.get("offers"))

    city = _first_string(address.get("addressLocality"), location.get("city"), raw_event.get("city"))
    country = _first_string(address.get("addressCountry"), location.get("country"), raw_event.get("country"))
    latitude = _as_float(raw_event.get("latitude") or location.get("latitude"))
    longitude = _as_float(raw_event.get("longitude") or location.get("longitude"))
    categories = raw_event.get("categories") if isinstance(raw_event.get("categories"), list) else []
    category = _first_string(*(categories or []), raw_event.get("eventType"))

    occurrence: GenericOccurrenceDict = {
        "locationText": _first_string(location.get("name"), address.get("streetAddress"), city),
        "latitude": latitude,
        "longitude": longitude,
        "venueName": _first_string(location.get("name")),
        "streetAddress": _first_string(address.get("streetAddress")),
        "city": city,
        "region": _first_string(address.get("addressRegion")),
        "country": country,
        "timezone": _first_string(raw_event.get("timezone")),
    }
    occurrence = {key: value for key, value in occurrence.items() if value not in (None, "", [], {})}  # type: ignore

    metadata = {
        **raw_event,
        "sourceProvider": "universe",
        "offers": offers,
    }

    event: GenericMappedEventDict = {
        "name": name,
        "sourceUrl": _first_string(raw_event.get("url"), raw_event.get("source_url")),
        "startAt": _first_string(raw_event.get("startDate")),
        "endAt": _first_string(raw_event.get("endDate")),
        "city": city,
        "country": country,
        "eventType": _first_string(raw_event.get("eventType"), category, "Event"),
        "category": category,
        "organizerName": _first_string(organizer.get("name")),
        "organizerWebsite": _first_string(organizer.get("url")),
        "organizerContact": _first_string(organizer.get("email"), organizer.get("phone")),
        "eventImageUrl": _full_universe_image_url(_first_string(raw_event.get("image"))),
        "industry": category,
        "description": _strip_html(raw_event.get("description")) or _first_string(raw_event.get("description")),
        "sourceProvider": "universe",
        "occurrence": occurrence,
        "metadataJson": metadata,
    }
    return {key: value for key, value in event.items() if value not in (None, "", [], {})}  # type: ignore


def _strip_large_universe_payload(value: Any) -> Any:
    if isinstance(value, dict):
        stripped: Dict[str, Any] = {}
        for key, item in value.items():
            if key in {"raw", "__typename"}:
                continue
            stripped[key] = _strip_large_universe_payload(item)
        return stripped
    if isinstance(value, list):
        return [_strip_large_universe_payload(item) for item in value]
    return value


def _sanitize_universe_event_for_eagle(event: Dict[str, Any]) -> Dict[str, Any]:
    sanitized = _strip_large_universe_payload(event)
    if not isinstance(sanitized, dict):
        return event

    description = sanitized.get("description")
    if isinstance(description, str) and len(description) > 4000:
        sanitized["description"] = description[:4000].rstrip()

    return sanitized


async def _search_universe_api_events(
    *,
    keyword: str,
    location: str,
    ll: Optional[str],
    limit: int,
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    location_query, latitude, longitude = _resolve_location(location, ll)
    per_page = min(max(limit, 1), UNIVERSE_DEFAULT_LIMIT_PER_PAGE)
    events: List[Dict[str, Any]] = []
    failures: List[Dict[str, Any]] = []

    async with httpx.AsyncClient(timeout=45, headers=UNIVERSE_HEADERS, follow_redirects=True) as client:
        first_total: Optional[int] = None
        max_pages = max(1, math.ceil(limit / per_page))
        page = 1
        while page <= max_pages and len(events) < limit:
            payload = {
                "operationName": "DiscoverSearchEvents",
                "variables": {
                    "latitude": latitude,
                    "longitude": longitude,
                    "limit": per_page,
                    "query": keyword or None,
                    "categories": None,
                    "time": None,
                    "freeEventsOnly": None,
                    "page": page,
                    "height": 365,
                    "width": 730,
                },
                "query": DISCOVER_SEARCH_EVENTS_QUERY,
            }
            try:
                data = await _post_graphql(client, UNIVERSE_DISCOVER_GRAPHQL_URL, payload)
            except Exception as error:
                failures.append({"page": page, "reason": str(error), "source": "api_search"})
                break

            result = data.get("events") if isinstance(data.get("events"), dict) else {}
            if first_total is None:
                total_count = result.get("totalCount")
                first_total = int(total_count) if isinstance(total_count, int) else None
                if first_total is not None:
                    max_pages = min(max_pages, max(1, math.ceil(first_total / per_page)))

            raw_events = result.get("events") if isinstance(result.get("events"), list) else []
            if not raw_events:
                break

            for raw_event in raw_events:
                if isinstance(raw_event, dict):
                    compact = _compact_universe_search_event(raw_event)
                    compact.setdefault("universe", {})["locationQuery"] = location_query
                    events.append(compact)
                    if len(events) >= limit:
                        break

            page += 1

    return events[:limit], failures


async def _fetch_universe_detail_event(
    client: httpx.AsyncClient,
    identifier: str,
) -> Optional[Dict[str, Any]]:
    payload = {
        "operationName": "CacheableEvent",
        "variables": {"id": identifier},
        "query": CACHEABLE_EVENT_QUERY,
    }
    data = await _post_graphql(client, UNIVERSE_GRAPHQL_URL, payload)
    event = data.get("event")
    if not isinstance(event, dict):
        return None
    return _compact_universe_detail_event(event)


async def crawl_universe_events_with_diagnostics(
    *,
    keyword: str = "music",
    location: str = "New York, NY, USA",
    ll: Optional[str] = "40.7127753,-74.0059728",
    limit: int = 50,
    source: UniverseSource = "auto",
    enrich_details: bool = True,
) -> Dict[str, Any]:
    parse_failures: List[Dict[str, Any]] = []

    if source == "html":
        return {
            "events": [],
            "parse_failures": [
                {
                    "reason": "html_shell_has_no_event_payload",
                    "source": "html",
                    "note": "Universe search/detail pages are React shells; use source=api or source=auto.",
                }
            ],
        }

    events, failures = await _search_universe_api_events(
        keyword=keyword,
        location=location,
        ll=ll,
        limit=limit,
    )
    parse_failures.extend(failures)

    if not enrich_details or not events:
        mapped_events = [mapped for event in events[:limit] if (mapped := _map_universe_event_to_generic(event))]
        return {"events": mapped_events, "parse_failures": parse_failures}

    enriched_events: List[Dict[str, Any]] = []
    async with httpx.AsyncClient(timeout=45, headers=UNIVERSE_HEADERS, follow_redirects=True) as client:
        for event in events:
            identifier = _search_result_identifier(event)
            if not identifier:
                parse_failures.append(
                    {
                        "event": event.get("name"),
                        "reason": "missing_universe_event_identifier",
                    }
                )
                enriched_events.append(event)
                continue

            try:
                detail = await _fetch_universe_detail_event(client, identifier)
                enriched_events.append(_merge_universe_events(event, detail) if detail else event)
            except Exception as error:
                parse_failures.append(
                    {
                        "event": event.get("name"),
                        "url": event.get("url") or event.get("source_url"),
                        "reason": str(error),
                        "source": "api_detail",
                    }
                )
                enriched_events.append(event)

    deduped: Dict[str, Dict[str, Any]] = {}
    for event in enriched_events:
        key = str(event.get("source_url") or event.get("url") or event.get("id") or len(deduped))
        deduped[key] = event

    return {
        "events": [mapped for event in list(deduped.values())[:limit] if (mapped := _map_universe_event_to_generic(event))],
        "parse_failures": parse_failures,
    }


def universe_explore_url(*, keyword: str, location: str, ll: Optional[str]) -> str:
    params = [f"query={quote(keyword)}", f"loc={quote(location)}"]
    if ll:
        params.append(f"ll={quote(ll)}")
    return f"{UNIVERSE_BASE_URL}/explore?{'&'.join(params)}"


async def ingest_universe_events_to_eagle(
    *,
    organization_id: Optional[str],
    workspace_id: Optional[str],
    events: List[Dict[str, Any]],
    parse_failures: Optional[List[Dict[str, Any]]] = None,
    persist: bool = False,
) -> Dict[str, Any]:
    return await ingest_generic_events_to_eagle(
        organization_id=organization_id,
        workspace_id=workspace_id,
        events=events,
        source_provider="universe",
        parse_failures=parse_failures,
        persist=persist,
        already_mapped=True,
    )
