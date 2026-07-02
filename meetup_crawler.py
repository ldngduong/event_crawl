import json
import logging
import os
import re
from datetime import datetime, timezone
from html import unescape
from typing import Any, Dict, List, Literal, Optional, Tuple
from urllib.parse import parse_qs, quote, unquote_plus, urljoin, urlparse

import httpx
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)

MeetupSource = Literal["auto", "api", "html"]

MEETUP_BASE_URL = "https://www.meetup.com"
MEETUP_GQL_URL = f"{MEETUP_BASE_URL}/gql2"
MEETUP_DEFAULT_PAGE_SIZE = 30
DEFAULT_EAGLE_API_BASE_URL = "http://localhost:3001/api/v1"
DEFAULT_EAGLE_IMPORT_BATCH_SIZE = 10

MEETUP_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/126.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json",
    "Content-Type": "application/json",
    "Origin": MEETUP_BASE_URL,
    "Referer": f"{MEETUP_BASE_URL}/find/",
}

MEETUP_LOCATION_PRESETS: Dict[str, Tuple[str, Optional[str], Optional[str], float, float]] = {
    "us--ny--new york": ("New York", "NY", "us", 40.7127753, -74.0059728),
    "new york": ("New York", "NY", "us", 40.7127753, -74.0059728),
    "new york, ny": ("New York", "NY", "us", 40.7127753, -74.0059728),
    "nyc": ("New York", "NY", "us", 40.7127753, -74.0059728),
    "au--nsw--sydney": ("Sydney", "NSW", "au", -33.8688197, 151.2092955),
    "sydney": ("Sydney", "NSW", "au", -33.8688197, 151.2092955),
}

FIND_EVENT_INFO_FRAGMENT = """
fragment FindEventInfo on Event {
  id
  title
  dateTime
  description
  eventType
  eventUrl
  isAttending
  isSaved
  maxTickets
  rsvpState
  featuredEventPhoto {
    baseUrl
    highResUrl
    id
  }
  displayPhoto {
    baseUrl
    highResUrl
    id
  }
  feeSettings {
    accepts
    currency
    amount
  }
  group {
    id
    name
    urlname
    timezone
    keyGroupPhoto {
      baseUrl
      highResUrl
      id
    }
    stats {
      eventRatings {
        average
        totalRatings
      }
    }
    isNewGroup
  }
  socialProofInsights {
    isTrendingEvent
    totalInterestedUsers
    interestedUsersSample {
      memberPhoto {
        id
        source: highResUrl
        baseUrl
      }
    }
  }
  rsvps {
    totalCount
    edges {
      node {
        isHost
        user: member {
          id
          name
          memberPhoto {
            id
            source: highResUrl
            baseUrl
          }
        }
      }
    }
  }
  series {
    description
    weeklyRecurrence {
      weeklyDaysOfWeek
      weeklyInterval
    }
    monthlyRecurrence {
      monthlyDayOfWeek
      monthlyWeekOfMonth
    }
    events(numberOfEvents: $numberOfEventsForSeries, startDate: $seriesStartDate) {
      edges {
        node {
          id
          dateTime
          isAttending
          group {
            urlname
          }
        }
      }
    }
  }
  venue {
    name
    address
    city
    state
    country
  }
}
"""

EVENT_SEARCH_WITH_SERIES_QUERY = (
    FIND_EVENT_INFO_FRAGMENT
    + """
query eventSearchWithSeries(
  $query: String!
  $lat: Float!
  $lon: Float!
  $startDateRange: DateTime
  $endDateRange: DateTime
  $eventType: EventType
  $radius: Float
  $isHappeningNow: Boolean
  $isStartingSoon: Boolean
  $categoryId: ID
  $topicCategoryId: ID
  $city: String
  $state: String
  $country: String
  $zip: String
  $sortField: KeywordSortField
  $first: Int
  $after: String
  $numberOfEventsForSeries: Int
  $seriesStartDate: Date
  $doConsolidateEvents: Boolean
  $dataConfiguration: String
  $rsvpCountRange: RsvpCountRange
  $minRsvpCount: Int
) {
  results: eventSearch(
    filter: {
      query: $query
      lat: $lat
      lon: $lon
      startDateRange: $startDateRange
      endDateRange: $endDateRange
      eventType: $eventType
      radius: $radius
      isHappeningNow: $isHappeningNow
      isStartingSoon: $isStartingSoon
      categoryId: $categoryId
      topicCategoryId: $topicCategoryId
      city: $city
      state: $state
      country: $country
      zip: $zip
      doConsolidateEvents: $doConsolidateEvents
      rsvpCountRange: $rsvpCountRange
      minRsvpCount: $minRsvpCount
    }
    first: $first
    after: $after
    sort: { sortField: $sortField }
    dataConfiguration: $dataConfiguration
  ) {
    pageInfo {
      hasNextPage
      endCursor
    }
    totalCount
    edges {
      node {
        ...FindEventInfo
        group {
          isNewGroup
        }
      }
      metadata {
        recId
        recSource
      }
    }
  }
}
"""
)


def _strip_html(value: Any) -> Optional[str]:
    if not isinstance(value, str):
        return None
    text = re.sub(r"<[^>]+>", " ", value)
    text = re.sub(r"\s+", " ", unescape(text)).strip()
    return text or None


def _as_list(value: Any) -> List[Any]:
    if value is None:
        return []
    return value if isinstance(value, list) else [value]


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
        graph = item.get("@graph")
        if isinstance(graph, list):
            events.extend(
                graph_item
                for graph_item in graph
                if isinstance(graph_item, dict) and _is_event_type(graph_item.get("@type"))
            )
    return events


def _json_loads_safe(value: str) -> Optional[Any]:
    try:
        return json.loads(value)
    except Exception:
        return None


def _clean_url(value: Optional[str]) -> Optional[str]:
    if not isinstance(value, str) or not value.strip():
        return None
    url = value.strip()
    parsed = urlparse(url)
    if not parsed.scheme:
        url = urljoin(MEETUP_BASE_URL, url)
        parsed = urlparse(url)
    return f"{parsed.scheme}://{parsed.netloc}{parsed.path}"


def _parse_search_url(search_url: Optional[str]) -> Dict[str, Any]:
    if not search_url:
        return {}
    parsed = urlparse(search_url)
    query = parse_qs(parsed.query)
    return {
        "keyword": unquote_plus((query.get("keywords") or [""])[0]) or None,
        "location": unquote_plus((query.get("location") or [""])[0]) or None,
        "source_url": search_url,
    }


def _decode_location_slug(location: str) -> Tuple[str, Optional[str], Optional[str], Optional[float], Optional[float]]:
    normalized = unquote_plus(location or "").strip()
    preset = MEETUP_LOCATION_PRESETS.get(normalized.lower())
    if preset:
        return preset

    parts = [part for part in normalized.split("--") if part]
    if len(parts) >= 3:
        country = parts[0].lower()
        state = parts[1].upper()
        city = parts[2].replace("+", " ").replace("-", " ").strip().title()
        return city, state, country, None, None

    return normalized or "New York", None, None, None, None


def _event_url_from_node(node: Dict[str, Any]) -> Optional[str]:
    event_url = node.get("eventUrl")
    if isinstance(event_url, str) and event_url:
        return _clean_url(event_url)

    group = node.get("group") if isinstance(node.get("group"), dict) else {}
    urlname = group.get("urlname")
    event_id = node.get("id")
    if urlname and event_id:
        return f"{MEETUP_BASE_URL}/{urlname}/events/{event_id}/"
    return None


def _image_from_node(node: Dict[str, Any]) -> Optional[str]:
    for key in ("featuredEventPhoto", "displayPhoto"):
        photo = node.get(key) if isinstance(node.get(key), dict) else {}
        for photo_key in ("highResUrl", "baseUrl"):
            value = photo.get(photo_key)
            if isinstance(value, str) and value:
                return value
    return None


def _compact_meetup_search_event(node: Dict[str, Any], metadata: Optional[Dict[str, Any]] = None) -> Optional[Dict[str, Any]]:
    name = node.get("title")
    if not isinstance(name, str) or not name.strip():
        return None

    group = node.get("group") if isinstance(node.get("group"), dict) else {}
    venue = node.get("venue") if isinstance(node.get("venue"), dict) else {}
    fee = node.get("feeSettings") if isinstance(node.get("feeSettings"), dict) else {}
    social = node.get("socialProofInsights") if isinstance(node.get("socialProofInsights"), dict) else {}
    rsvps = node.get("rsvps") if isinstance(node.get("rsvps"), dict) else {}
    event_url = _event_url_from_node(node)
    location = {
        "@type": "Place",
        "name": venue.get("name"),
        "address": {
            "@type": "PostalAddress",
            "streetAddress": venue.get("address"),
            "addressLocality": venue.get("city"),
            "addressRegion": venue.get("state"),
            "addressCountry": venue.get("country"),
        },
    }

    event = {
        "@type": "Event",
        "id": node.get("id"),
        "name": name.strip(),
        "title": name.strip(),
        "url": event_url,
        "source_url": event_url,
        "startDate": node.get("dateTime"),
        "endDate": node.get("endTime"),
        "timezone": group.get("timezone"),
        "eventType": node.get("eventType"),
        "description": _strip_html(node.get("description")),
        "image": _image_from_node(node),
        "location": location,
        "organizer": {
            "@type": "Organization",
            "name": group.get("name"),
            "url": f"{MEETUP_BASE_URL}/{group.get('urlname')}/" if group.get("urlname") else None,
        },
        "offers": {
            "price": fee.get("amount"),
            "priceCurrency": fee.get("currency"),
            "accepts": fee.get("accepts"),
        },
        "categories": [node.get("eventType")] if node.get("eventType") else [],
        "meetup": {
            "source": "search",
            "group": group,
            "rsvpsTotalCount": rsvps.get("totalCount"),
            "maxTickets": node.get("maxTickets"),
            "socialProofInsights": social,
            "metadata": metadata or {},
        },
    }
    return {key: value for key, value in event.items() if value not in (None, "", [], {})}


def _compact_meetup_json_ld_event(data: Dict[str, Any], source_url: str) -> Dict[str, Any]:
    location = data.get("location") if isinstance(data.get("location"), dict) else {}
    organizer = data.get("organizer") if isinstance(data.get("organizer"), dict) else {}
    image = data.get("image")
    if isinstance(image, list):
        image = image[0] if image else None

    return {
        "@type": "Event",
        "id": source_url.rstrip("/").split("/")[-1],
        "name": data.get("name"),
        "title": data.get("name"),
        "url": _clean_url(data.get("url")) or _clean_url(source_url),
        "source_url": _clean_url(data.get("url")) or _clean_url(source_url),
        "description": _strip_html(data.get("description")),
        "startDate": data.get("startDate"),
        "endDate": data.get("endDate"),
        "eventStatus": data.get("eventStatus"),
        "eventAttendanceMode": data.get("eventAttendanceMode"),
        "image": image,
        "location": location,
        "organizer": organizer,
        "eventType": "Meetup",
        "categories": ["Meetup"],
        "meetup": {
            "source": "json_ld_detail",
        },
    }


def _merge_event(base: Dict[str, Any], detail: Dict[str, Any]) -> Dict[str, Any]:
    merged = {**base, **{key: value for key, value in detail.items() if value not in (None, "", [], {})}}
    for key in ("location", "organizer", "offers", "meetup"):
        if isinstance(base.get(key), dict) or isinstance(detail.get(key), dict):
            merged[key] = {
                **(base.get(key) if isinstance(base.get(key), dict) else {}),
                **(detail.get(key) if isinstance(detail.get(key), dict) else {}),
            }
    return merged


def _strip_large_meetup_payload(value: Any) -> Any:
    if isinstance(value, dict):
        stripped: Dict[str, Any] = {}
        for key, item in value.items():
            if key in {"raw", "__typename"}:
                continue
            stripped[key] = _strip_large_meetup_payload(item)
        return stripped
    if isinstance(value, list):
        return [_strip_large_meetup_payload(item) for item in value]
    return value


def _sanitize_meetup_event_for_eagle(event: Dict[str, Any]) -> Dict[str, Any]:
    sanitized = _strip_large_meetup_payload(event)
    if not isinstance(sanitized, dict):
        return event

    description = sanitized.get("description")
    if isinstance(description, str) and len(description) > 4000:
        sanitized["description"] = description[:4000].rstrip()

    return sanitized


async def _post_meetup_graphql(client: httpx.AsyncClient, payload: Dict[str, Any]) -> Dict[str, Any]:
    response = await client.post(MEETUP_GQL_URL, json=payload)
    response.raise_for_status()
    body = response.json()
    if body.get("errors"):
        raise RuntimeError(json.dumps(body["errors"], ensure_ascii=False)[:1200])
    data = body.get("data")
    if not isinstance(data, dict):
        raise RuntimeError("missing_graphql_data")
    return data


async def _search_meetup_api_events(
    *,
    keyword: str,
    location: str,
    lat: Optional[float],
    lon: Optional[float],
    radius: Optional[float],
    limit: int,
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    city, state, country, preset_lat, preset_lon = _decode_location_slug(location)
    latitude = lat if lat is not None else preset_lat
    longitude = lon if lon is not None else preset_lon
    if latitude is None or longitude is None:
        return [], [
            {
                "reason": "missing_lat_lon_for_meetup_search",
                "location": location,
                "hint": "Pass lat/lon or use a known Meetup slug such as us--ny--New York.",
            }
        ]

    events: List[Dict[str, Any]] = []
    failures: List[Dict[str, Any]] = []
    cursor: Optional[str] = None
    first = min(MEETUP_DEFAULT_PAGE_SIZE, max(limit, 1))
    page = 0
    today = datetime.now(timezone.utc).date().isoformat()

    async with httpx.AsyncClient(timeout=45, headers=MEETUP_HEADERS, follow_redirects=True) as client:
        while len(events) < limit:
            payload = {
                "operationName": "eventSearchWithSeries",
                "variables": {
                    "query": keyword,
                    "lat": latitude,
                    "lon": longitude,
                    "startDateRange": None,
                    "endDateRange": None,
                    "eventType": None,
                    "radius": radius,
                    "isHappeningNow": None,
                    "isStartingSoon": None,
                    "categoryId": None,
                    "topicCategoryId": None,
                    "city": city,
                    "state": state,
                    "country": country,
                    "zip": None,
                    "sortField": None,
                    "first": min(first, limit - len(events)),
                    "after": cursor,
                    "numberOfEventsForSeries": 5,
                    "seriesStartDate": today,
                    "doConsolidateEvents": True,
                    "dataConfiguration": None,
                    "rsvpCountRange": None,
                    "minRsvpCount": None,
                },
                "query": EVENT_SEARCH_WITH_SERIES_QUERY,
            }
            try:
                data = await _post_meetup_graphql(client, payload)
            except Exception as error:
                failures.append({"page": page, "source": "api_search", "reason": str(error)})
                break

            result = data.get("results") if isinstance(data.get("results"), dict) else {}
            edges = result.get("edges") if isinstance(result.get("edges"), list) else []
            for edge in edges:
                if not isinstance(edge, dict):
                    continue
                node = edge.get("node") if isinstance(edge.get("node"), dict) else {}
                metadata = edge.get("metadata") if isinstance(edge.get("metadata"), dict) else {}
                compact = _compact_meetup_search_event(node, metadata)
                if compact:
                    events.append(compact)
                    if len(events) >= limit:
                        break

            page_info = result.get("pageInfo") if isinstance(result.get("pageInfo"), dict) else {}
            cursor = page_info.get("endCursor") if isinstance(page_info.get("endCursor"), str) else None
            has_next = bool(page_info.get("hasNextPage"))
            if not has_next or not cursor or not edges:
                break
            page += 1

    return events[:limit], failures


async def _fetch_meetup_detail_event(client: httpx.AsyncClient, url: str) -> Optional[Dict[str, Any]]:
    response = await client.get(url, headers={**MEETUP_HEADERS, "Accept": "text/html,application/xhtml+xml"})
    response.raise_for_status()
    soup = BeautifulSoup(response.text, "html.parser")
    for script in soup.find_all("script", {"type": "application/ld+json"}):
        payload = _json_loads_safe(script.get_text(" ", strip=True))
        if payload is None:
            continue
        for event in _iter_json_ld_events(payload):
            return _compact_meetup_json_ld_event(event, url)
    return None


async def _crawl_meetup_html_detail(search_url: Optional[str]) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    if not search_url or "/events/" not in urlparse(search_url).path:
        return [], [
            {
                "source": "html",
                "reason": "html_source_requires_detail_event_url",
                "hint": "Meetup /find search HTML does not contain events; use source=api/auto for search or pass a detail /events/{id}/ URL.",
            }
        ]

    async with httpx.AsyncClient(timeout=45, headers=MEETUP_HEADERS, follow_redirects=True) as client:
        try:
            event = await _fetch_meetup_detail_event(client, search_url)
            return ([event] if event else []), ([] if event else [{"url": search_url, "reason": "no_json_ld_event_found"}])
        except Exception as error:
            return [], [{"url": search_url, "source": "html_detail", "reason": str(error)}]


async def crawl_meetup_events_with_diagnostics(
    *,
    search_url: Optional[str] = None,
    keyword: str = "conference",
    location: str = "us--ny--New York",
    lat: Optional[float] = None,
    lon: Optional[float] = None,
    radius: Optional[float] = None,
    limit: int = 50,
    source: MeetupSource = "auto",
    enrich_details: bool = True,
) -> Dict[str, Any]:
    parsed = _parse_search_url(search_url)
    keyword = parsed.get("keyword") or keyword
    location = parsed.get("location") or location
    parse_failures: List[Dict[str, Any]] = []

    if source == "html":
        events, failures = await _crawl_meetup_html_detail(search_url)
        return {"events": events[:limit], "parse_failures": failures}

    events, failures = await _search_meetup_api_events(
        keyword=keyword,
        location=location,
        lat=lat,
        lon=lon,
        radius=radius,
        limit=limit,
    )
    parse_failures.extend(failures)

    if not enrich_details or not events:
        return {"events": events[:limit], "parse_failures": parse_failures}

    enriched: List[Dict[str, Any]] = []
    async with httpx.AsyncClient(timeout=45, headers=MEETUP_HEADERS, follow_redirects=True) as client:
        for event in events:
            source_url = event.get("source_url") or event.get("url")
            if not isinstance(source_url, str) or not source_url:
                enriched.append(event)
                continue
            try:
                detail = await _fetch_meetup_detail_event(client, source_url)
                enriched.append(_merge_event(event, detail) if detail else event)
            except Exception as error:
                parse_failures.append(
                    {
                        "url": source_url,
                        "event": event.get("name"),
                        "source": "detail_json_ld",
                        "reason": str(error),
                    }
                )
                enriched.append(event)

    deduped: Dict[str, Dict[str, Any]] = {}
    for event in enriched:
        key = str(event.get("source_url") or event.get("url") or event.get("id") or len(deduped))
        deduped[key] = event
    return {"events": list(deduped.values())[:limit], "parse_failures": parse_failures}


async def ingest_meetup_events_to_eagle(
    *,
    events: List[Dict[str, Any]],
    parse_failures: Optional[List[Dict[str, Any]]] = None,
    persist: bool = False,
) -> Dict[str, Any]:
    eagle_api_base_url = os.getenv("EAGLE_API_BASE_URL", DEFAULT_EAGLE_API_BASE_URL).rstrip("/")
    endpoint_url = os.getenv("EAGLE_MEETUP_IMPORT_URL") or f"{eagle_api_base_url}/scraper/events/meetup-import"
    batch_size = max(
        1,
        int(os.getenv("EAGLE_IMPORT_BATCH_SIZE", str(DEFAULT_EAGLE_IMPORT_BATCH_SIZE))),
    )
    results: List[Dict[str, Any]] = []
    failures: List[Dict[str, Any]] = []

    if not persist:
        return {
            "mode": "preview",
            "eagle_ingest_url": endpoint_url,
            "eagle_endpoint_url": endpoint_url,
            "crawled_count": len(events),
            "normalized_count": 0,
            "ingested_count": 0,
            "created_count": 0,
            "updated_count": 0,
            "skipped_count": 0,
            "failed_count": 0,
            "events": events,
            "results": [],
            "failures": [],
            "parse_failures": parse_failures or [],
        }

    async with httpx.AsyncClient(timeout=120) as client:
        for start in range(0, len(events), batch_size):
            batch = [_sanitize_meetup_event_for_eagle(event) for event in events[start : start + batch_size]]
            payload: Dict[str, Any] = {
                "events": batch,
                "parseFailures": parse_failures or [],
            }
            batch_meta = {
                "batch_start": start,
                "batch_end": start + len(batch) - 1,
                "event_count": len(batch),
                "source_urls": [
                    event.get("url") or event.get("source_url") or event.get("sourceUrl")
                    for event in batch[:5]
                ],
            }

            try:
                response = await client.post(endpoint_url, json=payload)
                response.raise_for_status()
                eagle_response = response.json()
                results.append({**batch_meta, "eagle_response": eagle_response})
                eagle_data = (
                    eagle_response.get("data")
                    if isinstance(eagle_response.get("data"), dict)
                    else eagle_response
                )
                failures.extend(eagle_data.get("failures") or [])
            except httpx.HTTPStatusError as error:
                failures.append(
                    {
                        **batch_meta,
                        "status_code": error.response.status_code,
                        "response": error.response.text,
                    }
                )
            except Exception as error:
                failures.append({**batch_meta, "error": str(error)})

    imported_count = 0
    created_count = 0
    updated_count = 0
    skipped_count = 0
    for result in results:
        eagle_response = result.get("eagle_response", {})
        eagle_data = (
            eagle_response.get("data")
            if isinstance(eagle_response.get("data"), dict)
            else eagle_response
        )
        imported_count += int(eagle_data.get("count") or 0)
        created_count += int(eagle_data.get("created") or 0)
        updated_count += int(eagle_data.get("updated") or 0)
        skipped_count += int(eagle_data.get("skipped") or 0)

    return {
        "mode": "persist",
        "eagle_ingest_url": endpoint_url,
        "eagle_endpoint_url": endpoint_url,
        "crawled_count": len(events),
        "normalized_count": imported_count,
        "ingested_count": imported_count,
        "created_count": created_count,
        "updated_count": updated_count,
        "skipped_count": skipped_count,
        "failed_count": len(failures),
        "events": events,
        "results": results,
        "failures": failures,
        "parse_failures": parse_failures or [],
    }
