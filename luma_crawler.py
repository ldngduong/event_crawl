import os
from typing import Any, Dict, List, Literal, Optional, Tuple
from urllib.parse import urlparse

import httpx

LumaSource = Literal["auto", "api", "html"]

LUMA_API_BASE_URL = "https://api.luma.com"
LUMA_WEB_BASE_URL = "https://lu.ma"
DEFAULT_EAGLE_API_BASE_URL = "http://localhost:3001/api/v1"
DEFAULT_EAGLE_IMPORT_BATCH_SIZE = 20

LUMA_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/126.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json",
    "Origin": LUMA_WEB_BASE_URL,
    "Referer": f"{LUMA_WEB_BASE_URL}/discover",
}


def _read_record(value: Any) -> Dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _read_list(value: Any) -> List[Any]:
    return value if isinstance(value, list) else []


def _first_string(*values: Any) -> Optional[str]:
    for value in values:
        if isinstance(value, str) and value.strip():
            return value.strip()
        if isinstance(value, (int, float)):
            return str(value)
    return None


def _as_float(value: Any) -> Optional[float]:
    try:
        if value in (None, ""):
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def _as_int(value: Any) -> Optional[int]:
    try:
        if value in (None, ""):
            return None
        return int(float(value))
    except (TypeError, ValueError):
        return None


def _luma_event_url(value: Any) -> Optional[str]:
    text = _first_string(value)
    if not text:
        return None
    if text.startswith("http://") or text.startswith("https://"):
        return text
    return f"{LUMA_WEB_BASE_URL}/{text.lstrip('/')}"


def _event_identifier_from_url(url: Optional[str]) -> Optional[str]:
    if not url:
        return None
    path = urlparse(url).path.strip("/")
    return path or None


def _compact_luma_event(entry: Dict[str, Any], category: Optional[Dict[str, Any]] = None) -> Optional[Dict[str, Any]]:
    event = _read_record(entry.get("event") or entry)
    if not event:
        return None

    name = _first_string(event.get("name"), event.get("title"))
    if not name:
        return None

    calendar = _read_record(entry.get("calendar"))
    geo = _read_record(event.get("geo_address_info"))
    coordinate = _read_record(event.get("coordinate"))
    virtual_info = _read_record(event.get("virtual_info"))
    source_url = _luma_event_url(event.get("url"))
    latitude = _as_float(coordinate.get("latitude"))
    longitude = _as_float(coordinate.get("longitude"))

    city = _first_string(geo.get("city"), geo.get("city_state"), _read_record(calendar.get("location")).get("city"))
    region = _first_string(geo.get("region"), geo.get("region_short"), _read_record(calendar.get("location")).get("region"))
    country = _first_string(geo.get("country"), _read_record(calendar.get("location")).get("country"))
    full_address = _first_string(geo.get("full_address"), geo.get("short_address"), geo.get("address"))
    timezone = _first_string(event.get("timezone"), calendar.get("timezone"), _read_record(calendar.get("location")).get("timezone"))
    category_name = _first_string(_read_record(category or {}).get("name"), event.get("event_type"))

    return {
        "@type": "Event",
        "id": _first_string(event.get("api_id")) or _event_identifier_from_url(source_url),
        "name": name,
        "url": source_url,
        "source_url": source_url,
        "startDate": _first_string(event.get("start_at")),
        "endDate": _first_string(event.get("end_at")),
        "timezone": timezone,
        "eventStatus": "https://schema.org/EventScheduled",
        "eventAttendanceMode": "https://schema.org/OfflineEventAttendanceMode"
        if event.get("location_type") == "offline"
        else "https://schema.org/OnlineEventAttendanceMode",
        "image": _first_string(event.get("social_image_url"), event.get("cover_url"), calendar.get("cover_image_url")),
        "description": _first_string(event.get("description"), calendar.get("description_short")),
        "location": {
            "@type": "Place",
            "name": _first_string(geo.get("description"), geo.get("short_address"), calendar.get("name")),
            "address": {
                "@type": "PostalAddress",
                "streetAddress": full_address,
                "addressLocality": city,
                "addressRegion": region,
                "postalCode": _first_string(geo.get("postal_code"), geo.get("zipcode")),
                "addressCountry": country,
            },
            "geo": {
                "@type": "GeoCoordinates",
                "latitude": latitude,
                "longitude": longitude,
            },
        },
        "organizer": {
            "@type": "Organization",
            "name": _first_string(calendar.get("name")),
            "url": _first_string(calendar.get("website")),
        },
        "city": city,
        "region": region,
        "country": country,
        "address": full_address,
        "latitude": latitude,
        "longitude": longitude,
        "category": category_name,
        "eventType": _first_string(event.get("event_type"), category_name, "Event"),
        "expectedAttendance": _as_int(entry.get("subscriber_count")),
        "luma": {
            "eventApiId": _first_string(event.get("api_id")),
            "calendarEventApiId": _first_string(entry.get("api_id")),
            "calendarApiId": _first_string(event.get("calendar_api_id"), calendar.get("api_id")),
            "calendarName": _first_string(calendar.get("name")),
            "calendarSlug": _first_string(calendar.get("slug")),
            "calendarWebsite": _first_string(calendar.get("website")),
            "categoryApiId": _first_string(_read_record(category or {}).get("api_id")),
            "categorySlug": _first_string(_read_record(category or {}).get("slug")),
            "categoryName": _first_string(_read_record(category or {}).get("name")),
            "locationType": _first_string(event.get("location_type")),
            "visibility": _first_string(event.get("visibility")),
            "virtualInfo": virtual_info or None,
            "geoAddressInfo": geo or None,
            "coordinate": coordinate or None,
        },
        "extraction_source": "luma_api",
    }


async def _get_json(client: httpx.AsyncClient, path: str, params: Dict[str, Any]) -> Dict[str, Any]:
    response = await client.get(f"{LUMA_API_BASE_URL}{path}", params={k: v for k, v in params.items() if v is not None}, headers=LUMA_HEADERS)
    response.raise_for_status()
    payload = response.json()
    return payload if isinstance(payload, dict) else {}


async def _fetch_category_calendars(
    client: httpx.AsyncClient,
    *,
    category_slug: str,
    max_calendars: int,
) -> Tuple[Optional[Dict[str, Any]], List[Dict[str, Any]]]:
    payload = await _get_json(client, "/discover/category/get-page", {"slug": category_slug})
    category = _read_record(payload.get("category"))
    calendar_candidates = [
        *_read_list(payload.get("timeline_calendars")),
        *_read_list(payload.get("featured_calendars")),
    ]

    calendars: List[Dict[str, Any]] = []
    seen_calendar_ids = set()
    for candidate in calendar_candidates:
        if not isinstance(candidate, dict):
            continue
        calendar = _read_record(candidate.get("calendar"))
        calendar_id = _first_string(calendar.get("api_id"), candidate.get("api_id"))
        if calendar_id and calendar_id in seen_calendar_ids:
            continue
        if calendar_id:
            seen_calendar_ids.add(calendar_id)
        calendars.append(candidate)
        if len(calendars) >= max_calendars:
            break

    return category or None, calendars


async def _fetch_calendar_events(
    client: httpx.AsyncClient,
    *,
    calendar_api_id: str,
    after: Optional[str],
    before: Optional[str],
    period: str,
    pagination_limit: int,
) -> List[Dict[str, Any]]:
    payload = await _get_json(
        client,
        "/calendar/get-items",
        {
            "calendar_api_id": calendar_api_id,
            "after": after,
            "before": before,
            "period": period,
            "pagination_limit": pagination_limit,
        },
    )
    return [entry for entry in _read_list(payload.get("entries")) if isinstance(entry, dict)]


async def crawl_luma_events_with_diagnostics(
    *,
    category_slug: Optional[str] = "tech",
    calendar_api_id: Optional[str] = None,
    after: Optional[str] = None,
    before: Optional[str] = None,
    period: str = "future",
    pagination_limit: int = 20,
    max_calendars: int = 10,
    limit: int = 50,
    source: LumaSource = "auto",
) -> Dict[str, Any]:
    if source == "html":
        return {
            "events": [],
            "parse_failures": [
                {
                    "source": "html",
                    "reason": "luma_html_not_required",
                    "hint": "Luma public discovery APIs expose category, calendar, and event payloads directly.",
                }
            ],
        }

    events: List[Dict[str, Any]] = []
    failures: List[Dict[str, Any]] = []
    diagnostics: Dict[str, Any] = {
        "category_slug": category_slug,
        "calendar_api_id": calendar_api_id,
        "period": period,
        "pagination_limit": pagination_limit,
        "max_calendars": max_calendars,
        "limit": limit,
        "category_calendar_count": 0,
        "scanned_calendar_count": 0,
        "calendar_event_counts": [],
    }

    async with httpx.AsyncClient(timeout=60, follow_redirects=True) as client:
        try:
            if calendar_api_id:
                entries = await _fetch_calendar_events(
                    client,
                    calendar_api_id=calendar_api_id,
                    after=after,
                    before=before,
                    period=period,
                    pagination_limit=min(max(1, pagination_limit), limit),
                )
                diagnostics["scanned_calendar_count"] = 1
                diagnostics["calendar_event_counts"].append(
                    {
                        "calendar_api_id": calendar_api_id,
                        "entry_count": len(entries),
                    }
                )
                for entry in entries:
                    compact = _compact_luma_event(entry)
                    if compact:
                        events.append(compact)
                return {"events": events[:limit], "parse_failures": failures, "diagnostics": diagnostics}

            if not category_slug:
                return {
                    "events": [],
                    "parse_failures": [
                        {
                            "source": "api",
                            "reason": "missing_luma_category_slug_or_calendar_api_id",
                        }
                    ],
                    "diagnostics": diagnostics,
                }

            category, calendars = await _fetch_category_calendars(
                client,
                category_slug=category_slug,
                max_calendars=max_calendars,
            )
            diagnostics["category_calendar_count"] = len(calendars)
            if not calendars:
                failures.append(
                    {
                        "source": "api",
                        "reason": "no_luma_calendars_found",
                        "category_slug": category_slug,
                    }
                )

            for calendar_entry in calendars:
                calendar = _read_record(calendar_entry.get("calendar"))
                cal_id = _first_string(calendar.get("api_id"))
                if not cal_id:
                    continue
                try:
                    entries = await _fetch_calendar_events(
                        client,
                        calendar_api_id=cal_id,
                        after=after,
                        before=before,
                        period=period,
                        pagination_limit=min(max(1, pagination_limit), max(1, limit - len(events))),
                    )
                    diagnostics["scanned_calendar_count"] += 1
                    diagnostics["calendar_event_counts"].append(
                        {
                            "calendar_api_id": cal_id,
                            "calendar_name": _first_string(calendar.get("name")),
                            "entry_count": len(entries),
                        }
                    )
                    for entry in entries:
                        enriched_entry = {**entry, "calendar": _read_record(entry.get("calendar")) or calendar}
                        compact = _compact_luma_event(enriched_entry, category)
                        if compact:
                            events.append(compact)
                            if len(events) >= limit:
                                break
                except Exception as error:
                    failures.append(
                        {
                            "source": "api",
                            "reason": str(error),
                            "calendar_api_id": cal_id,
                            "calendar_name": _first_string(calendar.get("name")),
                        }
                    )
                if len(events) >= limit:
                    break
        except Exception as error:
            failures.append(
                {
                    "source": "api",
                    "reason": str(error),
                    "category_slug": category_slug,
                    "calendar_api_id": calendar_api_id,
                }
            )

    deduped: Dict[str, Dict[str, Any]] = {}
    for event in events:
        key = str(event.get("source_url") or event.get("url") or event.get("id") or len(deduped))
        deduped[key] = event

    diagnostics["deduped_count"] = len(deduped)
    return {"events": list(deduped.values())[:limit], "parse_failures": failures, "diagnostics": diagnostics}


def _sanitize_luma_event_for_eagle(event: Dict[str, Any]) -> Dict[str, Any]:
    payload = dict(event)
    description = payload.get("description")
    if isinstance(description, str) and len(description) > 5000:
        payload["description"] = description[:5000]
    return payload


async def ingest_luma_events_to_eagle(
    *,
    events: List[Dict[str, Any]],
    parse_failures: Optional[List[Dict[str, Any]]] = None,
    diagnostics: Optional[Dict[str, Any]] = None,
    persist: bool = False,
) -> Dict[str, Any]:
    eagle_api_base_url = os.getenv("EAGLE_API_BASE_URL", DEFAULT_EAGLE_API_BASE_URL).rstrip("/")
    endpoint_url = f"{eagle_api_base_url}/scraper/events/luma-import"
    batch_size = max(1, int(os.getenv("EAGLE_IMPORT_BATCH_SIZE", str(DEFAULT_EAGLE_IMPORT_BATCH_SIZE))))
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
            "diagnostics": diagnostics or {},
        }

    async with httpx.AsyncClient(timeout=120) as client:
        for start in range(0, len(events), batch_size):
            batch = [_sanitize_luma_event_for_eagle(event) for event in events[start : start + batch_size]]
            batch_meta = {
                "batch_start": start,
                "batch_end": start + len(batch) - 1,
                "event_count": len(batch),
                "source_urls": [event.get("url") or event.get("source_url") for event in batch[:5]],
            }
            try:
                response = await client.post(endpoint_url, json={"events": batch, "parseFailures": parse_failures or []})
                response.raise_for_status()
                eagle_response = response.json()
                results.append({**batch_meta, "eagle_response": eagle_response})
                eagle_data = eagle_response.get("data") if isinstance(eagle_response.get("data"), dict) else eagle_response
                if isinstance(eagle_data, dict) and eagle_data.get("failures"):
                    for failure in eagle_data["failures"]:
                        failures.append({**batch_meta, "failure": failure})
            except httpx.HTTPStatusError as error:
                failures.append(
                    {
                        **batch_meta,
                        "status_code": error.response.status_code,
                        "response": error.response.text[:2000],
                    }
                )
            except Exception as error:
                failures.append({**batch_meta, "reason": str(error)})

    created_count = 0
    updated_count = 0
    skipped_count = 0
    ingested_count = 0
    for result in results:
        response = result.get("eagle_response")
        data = response.get("data") if isinstance(response, dict) and isinstance(response.get("data"), dict) else response
        if not isinstance(data, dict):
            continue
        created_count += _as_int(data.get("created")) or 0
        updated_count += _as_int(data.get("updated")) or 0
        skipped_count += _as_int(data.get("skipped")) or 0
        ingested_count += _as_int(data.get("count")) or 0

    return {
        "mode": "persist",
        "eagle_ingest_url": endpoint_url,
        "eagle_endpoint_url": endpoint_url,
        "crawled_count": len(events),
        "normalized_count": ingested_count,
        "ingested_count": ingested_count,
        "created_count": created_count,
        "updated_count": updated_count,
        "skipped_count": skipped_count,
        "failed_count": len(failures),
        "events": events,
        "results": results,
        "failures": failures,
        "parse_failures": parse_failures or [],
        "diagnostics": diagnostics or {},
    }
