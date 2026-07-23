import asyncio
import json
import logging
import math
import os
import re
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Literal, Optional, Tuple
from urllib.parse import urlencode, urljoin, urlparse, urlunparse, parse_qs

os.environ.setdefault(
    "CRAWL4_AI_BASE_DIRECTORY",
    str(Path(__file__).resolve().parent / ".crawl4ai_data"),
)

import httpx
from bs4 import BeautifulSoup
from crawl4ai import AsyncWebCrawler, BrowserConfig, CacheMode, CrawlerRunConfig
from generic_mapper import ingest_generic_events_to_eagle
from schemas import GenericMappedEventDict, GenericOccurrenceDict

logger = logging.getLogger(__name__)

ICA_BASE_URL = "https://internationalconferencealerts.com"
ICA_EVENT_URL_PATTERN = re.compile(
    r"https?://internationalconferencealerts\.com/event-[^\s\"'<>]+",
    re.IGNORECASE,
)
ICA_EVENT_PATH_PATTERN = re.compile(r"^/event-[^/]+$")
ICA_SOURCE = Literal["auto", "api", "html", "sitemap"]
DEFAULT_EAGLE_API_BASE_URL = "http://localhost:3001/api/v1"
DEFAULT_GEOCODING_URL = f"{DEFAULT_EAGLE_API_BASE_URL}/geocoding/address"
DEFAULT_EAGLE_IMPORT_BATCH_SIZE = 20
BRIGHTDATA_REQUEST_URL = "https://api.brightdata.com/request"

_cached_events: Dict[str, Dict[str, Any]] = {}


def _resolve_proxy_url(proxy_url: Optional[str] = None) -> Optional[str]:
    value = proxy_url or os.getenv("CRAWLER_PROXY_URL") or os.getenv("PLAYWRIGHT_PROXY_URL")
    return value.strip() if isinstance(value, str) and value.strip() else None


def _browser_config(proxy_url: Optional[str] = None) -> BrowserConfig:
    resolved_proxy_url = _resolve_proxy_url(proxy_url)
    return BrowserConfig(
        headless=True,
        verbose=False,
        viewport_width=1440,
        viewport_height=1200,
        proxy=resolved_proxy_url,
        enable_stealth=True,
        user_agent=(
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/126.0.0.0 Safari/537.36"
        ),
    )


def _headers(referer: Optional[str] = None, accept: Optional[str] = None) -> Dict[str, str]:
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/126.0.0.0 Safari/537.36"
        ),
        "Accept": accept or "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
    }
    if referer:
        headers["Referer"] = referer
    return headers


def _brightdata_api_key() -> Optional[str]:
    return (
        os.getenv("BRIGHTDATA_API_KEY")
        or os.getenv("BRIGHT_DATA_API_KEY")
        or os.getenv("BRIGHTDATA_TOKEN")
    )


def _brightdata_zone() -> str:
    return os.getenv("BRIGHTDATA_ZONE") or os.getenv("BRIGHT_DATA_ZONE") or "web_unlocker1"


async def _fetch_via_brightdata(
    client: httpx.AsyncClient,
    url: str,
    *,
    source: str,
    accept: Optional[str] = None,
) -> Tuple[Optional[str], Optional[Dict[str, Any]]]:
    api_key = _brightdata_api_key()
    if not api_key:
        return None, {
            "source": source,
            "url": url,
            "reason": "brightdata_api_key_missing",
            "env": "BRIGHTDATA_API_KEY",
        }

    try:
        response = await client.post(
            BRIGHTDATA_REQUEST_URL,
            headers={
                "Content-Type": "application/json",
                "Authorization": f"Bearer {api_key}",
            },
            json={
                "zone": _brightdata_zone(),
                "url": url,
                "format": "raw",
            },
            timeout=90,
        )
        if response.status_code >= 400:
            return None, {
                "source": source,
                "url": url,
                "status": response.status_code,
                "reason": "brightdata_request_failed",
                "response": response.text[:1000],
            }
        text = response.text
        if _cloudflare_blocked(text, response.status_code):
            return None, {
                "source": source,
                "url": url,
                "status": response.status_code,
                "reason": "brightdata_returned_cloudflare_challenge",
            }
        if accept and "json" in accept:
            content_type = response.headers.get("content-type", "")
            if "json" not in content_type and not text.lstrip().startswith(("{", "[")):
                return None, {
                    "source": source,
                    "url": url,
                    "status": response.status_code,
                    "reason": f"brightdata_non_json_content_type:{content_type}",
                }
        return text, None
    except Exception as error:
        return None, {"source": source, "url": url, "reason": str(error)}


def _clean_url(url: str) -> str:
    parsed = urlparse(urljoin(ICA_BASE_URL, url))
    return urlunparse((parsed.scheme, parsed.netloc, parsed.path.rstrip("/"), "", "", ""))


def _json_loads_safe(value: str) -> Optional[Any]:
    try:
        return json.loads(value)
    except Exception:
        return None


def _as_list(value: Any) -> List[Any]:
    if value is None:
        return []
    return value if isinstance(value, list) else [value]


def _is_event_type(value: Any) -> bool:
    if isinstance(value, list):
        return any(_is_event_type(item) for item in value)
    return str(value).lower() == "event"


def _iter_json_ld_events(payload: Any) -> Iterable[Dict[str, Any]]:
    for item in _as_list(payload):
        if not isinstance(item, dict):
            continue
        if _is_event_type(item.get("@type")):
            yield item
        graph = item.get("@graph")
        if isinstance(graph, list):
            for graph_item in graph:
                if isinstance(graph_item, dict) and _is_event_type(graph_item.get("@type")):
                    yield graph_item
        item_list = item.get("itemListElement")
        if isinstance(item_list, list):
            for wrapper in item_list:
                event = wrapper.get("item") if isinstance(wrapper, dict) else None
                if isinstance(event, dict) and _is_event_type(event.get("@type")):
                    yield event


def _first_string(*values: Any) -> Optional[str]:
    for value in values:
        if isinstance(value, str) and value.strip():
            return value.strip()
        if isinstance(value, (int, float)) and not isinstance(value, bool):
            return str(value)
    return None


def _first_number(*values: Any) -> Optional[float]:
    for value in values:
        try:
            if value is not None and str(value).strip() != "":
                number = float(value)
                if math.isfinite(number):
                    return number
        except Exception:
            continue
    return None


def _first_int(*values: Any) -> Optional[int]:
    number = _first_number(*values)
    return int(number) if number is not None else None


def _first_url_or_string(value: Any) -> Optional[str]:
    if isinstance(value, str):
        return value.strip() or None
    if isinstance(value, list):
        for item in value:
            text = _first_url_or_string(item)
            if text:
                return text
    if isinstance(value, dict):
        return _first_string(value.get("url"), value.get("@id"), value.get("content"))
    return None


def _strip_html(value: Any) -> Optional[str]:
    if not isinstance(value, str):
        return None
    return BeautifulSoup(value, "html.parser").get_text(" ", strip=True) or None


def _parse_slug_metadata(url: str) -> Dict[str, Optional[str]]:
    path = urlparse(url).path.strip("/")
    parts = path.split("-")
    if not path.startswith("event-") or len(parts) < 5:
        return {}

    external_id = parts[-1]
    yyyymm = parts[-2] if re.fullmatch(r"\d{6}", parts[-2]) else None
    country = parts[-3] if len(parts) >= 3 else None
    city = parts[-4] if len(parts) >= 4 else None
    acronym = "-".join(parts[1:-4]) if yyyymm and len(parts) > 5 else None

    return {
        "id": external_id,
        "slug": path,
        "acronym": acronym,
        "city": city.replace("-", " ").title() if city else None,
        "country": country.upper() if country else None,
        "month": yyyymm,
    }


def _compact_slug_event(url: str) -> Dict[str, Any]:
    clean_source_url = _clean_url(url)
    slug = _parse_slug_metadata(clean_source_url)
    acronym = slug.get("acronym")
    title = acronym.upper() if acronym else slug.get("slug") or clean_source_url
    return {
        "@type": "Event",
        "id": slug.get("id") or clean_source_url,
        "name": title,
        "title": title,
        "url": clean_source_url,
        "source_url": clean_source_url,
        "location": {"name": slug.get("city")},
        "country": slug.get("country"),
        "eventType": "Conference",
        "internationalConferenceAlerts": {
            "source": "slug_fallback",
            "slug": slug.get("slug"),
            "acronym": slug.get("acronym"),
            "month": slug.get("month"),
        },
    }


def _normalize_location(raw_location: Any) -> Dict[str, Any]:
    if isinstance(raw_location, str):
        return {"name": raw_location}
    if not isinstance(raw_location, dict):
        return {}

    address = raw_location.get("address")
    normalized_address: Dict[str, Any] = {}
    if isinstance(address, dict):
        normalized_address = {
            "streetAddress": _first_string(address.get("streetAddress")),
            "addressLocality": _first_string(address.get("addressLocality")),
            "addressRegion": _first_string(address.get("addressRegion")),
            "postalCode": _first_string(address.get("postalCode")),
            "addressCountry": _first_string(address.get("addressCountry")),
        }
    elif isinstance(address, str):
        normalized_address = {"streetAddress": address}

    geo = raw_location.get("geo") if isinstance(raw_location.get("geo"), dict) else {}
    return {
        "name": _first_string(raw_location.get("name")),
        "address": {key: value for key, value in normalized_address.items() if value},
        "latitude": _first_number(raw_location.get("latitude"), geo.get("latitude")),
        "longitude": _first_number(raw_location.get("longitude"), geo.get("longitude")),
    }


def _compact_json_ld_event(event: Dict[str, Any], source_url: str) -> Dict[str, Any]:
    clean_source_url = _clean_url(_first_string(event.get("url"), source_url) or source_url)
    slug = _parse_slug_metadata(clean_source_url)
    organizer = event.get("organizer") if isinstance(event.get("organizer"), dict) else {}
    offers = event.get("offers") if isinstance(event.get("offers"), dict) else {}
    location = _normalize_location(event.get("location"))
    categories = event.get("keywords")
    if isinstance(categories, str):
        category_list = [part.strip() for part in re.split(r"[,|]", categories) if part.strip()]
    elif isinstance(categories, list):
        category_list = [str(part).strip() for part in categories if str(part).strip()]
    else:
        category_list = []

    organizer_name = _first_string(organizer.get("name"))
    organizer_url = _first_string(organizer.get("url"))
    organizer_email = _first_string(organizer.get("email"))
    organizer_phone = _first_string(organizer.get("telephone"), organizer.get("phone"))
    organizer_contact = _first_string(
        event.get("organizerContact"),
        " | ".join(part for part in (organizer_email, organizer_phone) if part),
    )
    event_image_url = _first_url_or_string(event.get("image"))
    industry = _first_string(event.get("industry"), event.get("about"))

    return {
        "@type": "Event",
        "id": _first_string(event.get("@id"), event.get("identifier"), slug.get("id"), clean_source_url),
        "name": _first_string(event.get("name"), event.get("headline"), slug.get("acronym")),
        "title": _first_string(event.get("name"), event.get("headline"), slug.get("acronym")),
        "url": clean_source_url,
        "source_url": clean_source_url,
        "startDate": _first_string(event.get("startDate")),
        "endDate": _first_string(event.get("endDate")),
        "description": _strip_html(event.get("description")),
        "image": event_image_url,
        "eventImageUrl": event_image_url,
        "location": location,
        "organizer": {
            "@type": "Organization",
            "name": organizer_name,
            "url": organizer_url,
            "email": organizer_email,
            "telephone": organizer_phone,
        },
        "organizerName": organizer_name,
        "organizerWebsite": organizer_url,
        "organizerContact": organizer_contact,
        "offers": {
            "price": offers.get("price"),
            "priceCurrency": offers.get("priceCurrency"),
            "url": offers.get("url"),
        },
        "categories": category_list,
        "industry": industry or (category_list[0] if category_list else None),
        "eventType": category_list[0] if category_list else "Conference",
        "expectedAttendance": _first_int(
            event.get("maximumAttendeeCapacity"),
            event.get("remainingAttendeeCapacity"),
            event.get("attendeeCount"),
            event.get("expectedAttendance"),
        ),
        "hotelFitScore": _first_number(event.get("hotelFitScore"), event.get("hotel_fit_score")),
        "priorityExplanation": _first_string(
            event.get("priorityExplanation"),
            event.get("priority_explanation"),
        ),
        "country": _first_string(location.get("address", {}).get("addressCountry"), slug.get("country")),
        "internationalConferenceAlerts": {
            "source": "json_ld",
            "slug": slug.get("slug"),
            "acronym": slug.get("acronym"),
            "month": slug.get("month"),
        },
    }


def _extract_json_ld_events(html: str, source_url: str) -> List[Dict[str, Any]]:
    soup = BeautifulSoup(html or "", "html.parser")
    events: List[Dict[str, Any]] = []
    for script in soup.find_all("script", attrs={"type": "application/ld+json"}):
        payload = _json_loads_safe(script.string or script.get_text(" ", strip=True) or "")
        for event in _iter_json_ld_events(payload):
            events.append(_compact_json_ld_event(event, source_url))
    return events


def _extract_event_links(html: str) -> List[str]:
    urls: List[str] = []
    soup = BeautifulSoup(html or "", "html.parser")
    for link in soup.find_all("a", href=True):
        href = str(link.get("href") or "")
        clean = _clean_url(href)
        if ICA_EVENT_PATH_PATTERN.match(urlparse(clean).path) and clean not in urls:
            urls.append(clean)
    for match in ICA_EVENT_URL_PATTERN.finditer(html or ""):
        clean = _clean_url(match.group(0))
        if clean not in urls:
            urls.append(clean)
    return urls


def _extract_card_context(html: str) -> None:
    soup = BeautifulSoup(html or "", "html.parser")
    for anchor in soup.find_all("a", href=True):
        href = str(anchor.get("href") or "")
        clean = _clean_url(href)
        if not ICA_EVENT_PATH_PATTERN.match(urlparse(clean).path):
            continue

        card = anchor
        for parent in anchor.parents:
            if getattr(parent, "attrs", {}).get("data-slot") == "card":
                card = parent
                break

        text = " ".join(card.get_text(" ", strip=True).split())
        title = None
        heading = card.find(["h1", "h2", "h3", "h4"]) if hasattr(card, "find") else None
        if heading:
            title = heading.get_text(" ", strip=True)
        event = _cached_events.get(clean) or _compact_slug_event(clean)
        event.update(
            {
                "name": title or event.get("name"),
                "title": title or event.get("title"),
                "eventType": "Conference",
                "industry": _first_string(event.get("industry"), _infer_industry_from_text(text)),
                "internationalConferenceAlerts": {
                    **(event.get("internationalConferenceAlerts") if isinstance(event.get("internationalConferenceAlerts"), dict) else {}),
                    "source": "html_card",
                    "cardText": text[:1000],
                },
            }
        )
        _cached_events[clean] = {key: value for key, value in event.items() if value not in (None, "", [], {})}


def _infer_industry_from_text(text: str) -> Optional[str]:
    normalized = text.lower()
    candidates = (
        ("engineering", "Engineering And Technology"),
        ("technology", "Engineering And Technology"),
        ("business", "Business And Economics"),
        ("economics", "Business And Economics"),
        ("education", "Education"),
        ("medical", "Medical And Health Science"),
        ("health", "Medical And Health Science"),
        ("social science", "Social Sciences And Humanities"),
        ("arts", "Arts And Humanities"),
        ("law", "Law"),
    )
    for needle, label in candidates:
        if needle in normalized:
            return label
    return None


def _extract_meta_fallback_event(html: str, source_url: str) -> Optional[Dict[str, Any]]:
    soup = BeautifulSoup(html or "", "html.parser")
    title = _first_string(
        soup.find("meta", attrs={"property": "og:title"}).get("content")
        if soup.find("meta", attrs={"property": "og:title"})
        else None,
        soup.find("h1").get_text(" ", strip=True) if soup.find("h1") else None,
        soup.title.get_text(" ", strip=True) if soup.title else None,
    )
    if not title or "just a moment" in title.lower():
        return None

    description = _first_string(
        soup.find("meta", attrs={"name": "description"}).get("content")
        if soup.find("meta", attrs={"name": "description"})
        else None,
        soup.find("meta", attrs={"property": "og:description"}).get("content")
        if soup.find("meta", attrs={"property": "og:description"})
        else None,
    )
    clean_source_url = _clean_url(source_url)
    slug = _parse_slug_metadata(clean_source_url)
    return {
        "@type": "Event",
        "id": slug.get("id") or clean_source_url,
        "name": re.sub(r"\s*\|\s*International Conference Alerts.*$", "", title).strip(),
        "title": re.sub(r"\s*\|\s*International Conference Alerts.*$", "", title).strip(),
        "url": clean_source_url,
        "source_url": clean_source_url,
        "description": description,
        "location": {"name": slug.get("city")},
        "country": slug.get("country"),
        "eventType": "Conference",
        "internationalConferenceAlerts": {
            "source": "meta_fallback",
            "slug": slug.get("slug"),
            "acronym": slug.get("acronym"),
            "month": slug.get("month"),
        },
    }


def _build_search_url(
    *,
    search_url: Optional[str],
    q: Optional[str],
    country: Optional[str],
    month: Optional[str],
    topic_slug: Optional[str],
    subtopic_slug: Optional[str],
    city_slug: Optional[str],
    page: int,
) -> str:
    if search_url:
        return search_url

    path_slug = subtopic_slug or topic_slug or city_slug
    path = f"/{path_slug.strip('/')}" if path_slug else "/conferences"
    query: Dict[str, str] = {}
    if q:
        query["q"] = q
    if country:
        query["country"] = country
    if month:
        query["month"] = month
    if page > 1:
        query["page"] = str(page)
    suffix = f"?{urlencode(query)}" if query else ""
    return f"{ICA_BASE_URL}{path}{suffix}"


def _cloudflare_blocked(text: str, status_code: int) -> bool:
    lowered = (text or "").lower()
    return status_code in {403, 429, 503} and (
        "cf_chl_opt" in lowered
        or "cloudflare" in lowered
        or "just a moment" in lowered
    )


def _normalize_api_event(raw: Dict[str, Any], source_url: str) -> Optional[Dict[str, Any]]:
    name = _first_string(raw.get("name"), raw.get("title"), raw.get("event_name"))
    url = _first_string(raw.get("url"), raw.get("source_url"), raw.get("link"), raw.get("slug"))
    if url and not url.startswith("http"):
        url = urljoin(ICA_BASE_URL, url)
    clean_url = _clean_url(url or source_url)
    if not name and not ICA_EVENT_PATH_PATTERN.match(urlparse(clean_url).path):
        return None

    latitude = _first_number(raw.get("latitude"), raw.get("lat"))
    longitude = _first_number(raw.get("longitude"), raw.get("lng"), raw.get("lon"))
    city = _first_string(raw.get("city"), raw.get("venue_city"))
    country = _first_string(raw.get("country"), raw.get("country_code"))
    venue = _first_string(raw.get("venue"), raw.get("venue_name"), raw.get("location"))

    return {
        "@type": "Event",
        "id": _first_string(raw.get("id"), raw.get("event_id"), _parse_slug_metadata(clean_url).get("id"), clean_url),
        "name": name or _parse_slug_metadata(clean_url).get("acronym"),
        "title": name or _parse_slug_metadata(clean_url).get("acronym"),
        "url": clean_url,
        "source_url": clean_url,
        "startDate": _first_string(raw.get("startDate"), raw.get("start_date"), raw.get("date")),
        "endDate": _first_string(raw.get("endDate"), raw.get("end_date")),
        "description": _strip_html(raw.get("description")),
        "location": {
            "name": venue or city,
            "latitude": latitude,
            "longitude": longitude,
            "address": {
                "addressLocality": city,
                "addressCountry": country,
            },
        },
        "country": country,
        "eventType": _first_string(raw.get("eventType"), raw.get("topic"), raw.get("category")) or "Conference",
        "internationalConferenceAlerts": {"source": "api_candidate", "raw": raw},
    }


def _collect_api_event_candidates(payload: Any) -> List[Dict[str, Any]]:
    if isinstance(payload, list):
        return [item for item in payload if isinstance(item, dict)]
    if not isinstance(payload, dict):
        return []
    for key in ("events", "data", "results", "items", "conferences"):
        value = payload.get(key)
        if isinstance(value, list):
            return [item for item in value if isinstance(item, dict)]
        if isinstance(value, dict):
            nested = _collect_api_event_candidates(value)
            if nested:
                return nested
    return []


async def _probe_api_candidates(
    *,
    client: httpx.AsyncClient,
    params: Dict[str, Any],
    limit: int,
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    endpoints = [
        "/api/listings/conferences",
        "/api/conferences",
        "/api/events",
        "/api/search",
        "/api/conference-alerts",
        "/api/conferences/search",
    ]
    failures: List[Dict[str, Any]] = []
    events: List[Dict[str, Any]] = []

    for path in endpoints:
        url = f"{ICA_BASE_URL}{path}"
        try:
            response = await client.get(
                url,
                params={key: value for key, value in params.items() if value not in (None, "")},
                headers=_headers(accept="application/json,text/plain,*/*"),
            )
            content_type = response.headers.get("content-type", "")
            if _cloudflare_blocked(response.text, response.status_code):
                unlocked_text, unlock_failure = await _fetch_via_brightdata(
                    client,
                    str(response.url),
                    source="brightdata_api_probe",
                    accept="application/json,text/plain,*/*",
                )
                if unlock_failure:
                    failures.append(
                        {
                            "source": "api_probe",
                            "endpoint": url,
                            "status": response.status_code,
                            "reason": "cloudflare_managed_challenge",
                            "unlocker": unlock_failure,
                        }
                    )
                    continue
                payload = _json_loads_safe(unlocked_text or "")
                raw_items = _collect_api_event_candidates(payload)
                for raw in raw_items:
                    compact = _normalize_api_event(raw, url)
                    if compact:
                        events.append(compact)
                        if len(events) >= limit:
                            return events[:limit], failures
                continue
            if response.status_code == 404:
                failures.append({"source": "api_probe", "endpoint": url, "status": 404, "reason": "not_found"})
                continue
            response.raise_for_status()
            if "json" not in content_type:
                failures.append({"source": "api_probe", "endpoint": url, "status": response.status_code, "reason": f"non_json_content_type:{content_type}"})
                continue
            raw_items = _collect_api_event_candidates(response.json())
            for raw in raw_items:
                compact = _normalize_api_event(raw, url)
                if compact:
                    events.append(compact)
                    if len(events) >= limit:
                        return events[:limit], failures
        except Exception as error:
            failures.append({"source": "api_probe", "endpoint": url, "reason": str(error)})

    return events[:limit], failures


async def _fetch_sitemap_urls(
    *,
    client: httpx.AsyncClient,
    q: Optional[str],
    country: Optional[str],
    month: Optional[str],
    limit: int,
) -> Tuple[List[str], List[Dict[str, Any]]]:
    failures: List[Dict[str, Any]] = []
    urls: List[str] = []
    try:
        index = await client.get(f"{ICA_BASE_URL}/sitemaps/events.xml", headers=_headers())
        index.raise_for_status()
        sitemap_urls = re.findall(r"<loc>(.*?)</loc>", index.text)
    except Exception as error:
        return [], [{"source": "sitemap_index", "reason": str(error)}]

    q_norm = q.lower().strip() if q else None
    country_norm = country.lower().strip() if country else None
    month_norm = re.sub(r"[^0-9]", "", month or "")

    for sitemap_url in sitemap_urls:
        if len(urls) >= limit:
            break
        try:
            response = await client.get(sitemap_url, headers=_headers(), timeout=30)
            response.raise_for_status()
        except Exception as error:
            failures.append({"source": "sitemap_part", "url": sitemap_url, "reason": str(error)})
            continue

        for loc in re.findall(r"<loc>(.*?)</loc>", response.text):
            clean = _clean_url(loc)
            slug = _parse_slug_metadata(clean)
            searchable = " ".join(str(value or "") for value in (slug.get("slug"), slug.get("acronym"), slug.get("city"))).lower()
            if q_norm and q_norm not in searchable:
                continue
            if country_norm and country_norm not in str(slug.get("country") or "").lower():
                continue
            if month_norm and not str(slug.get("month") or "").startswith(month_norm[:6]):
                continue
            if clean not in urls:
                urls.append(clean)
                if len(urls) >= limit:
                    break

    return urls[:limit], failures


async def _search_html_urls(
    *,
    client: httpx.AsyncClient,
    search_url: str,
    limit: int,
) -> Tuple[List[str], List[Dict[str, Any]]]:
    failures: List[Dict[str, Any]] = []
    try:
        response = await client.get(search_url, headers=_headers(), timeout=30)
        unlocked = False
        if _cloudflare_blocked(response.text, response.status_code):
            unlocked_html, unlock_failure = await _fetch_via_brightdata(
                client,
                str(response.url),
                source="brightdata_static_html_search",
            )
            if unlock_failure:
                return [], [
                    {
                        "source": "static_html_search",
                        "url": search_url,
                        "status": response.status_code,
                        "reason": "cloudflare_managed_challenge",
                        "unlocker": unlock_failure,
                    }
                ]
            response_text = unlocked_html or ""
            unlocked = True
        else:
            response_text = response.text
        if not unlocked:
            response.raise_for_status()
    except Exception as error:
        unlocked_html, unlock_failure = await _fetch_via_brightdata(
            client,
            search_url,
            source="brightdata_static_html_search",
        )
        if unlock_failure:
            return [], [
                {
                    "source": "static_html_search",
                    "url": search_url,
                    "reason": str(error),
                    "unlocker": unlock_failure,
                }
            ]
        response_text = unlocked_html or ""

    _extract_card_context(response_text)
    urls = _extract_event_links(response_text)
    for event in _extract_json_ld_events(response_text, search_url):
        event_url = _first_string(event.get("source_url"), event.get("url"))
        if event_url:
            clean = _clean_url(event_url)
            _cached_events[clean] = event
            if clean not in urls:
                urls.append(clean)
    return urls[:limit], failures


async def _search_browser_urls(search_url: str, limit: int, proxy_url: Optional[str] = None) -> Tuple[List[str], List[Dict[str, Any]]]:
    browser_config = _browser_config(proxy_url)
    run_config = CrawlerRunConfig(
        cache_mode=CacheMode.BYPASS,
        wait_until="networkidle",
        delay_before_return_html=3.0,
        page_timeout=90000,
        scan_full_page=True,
        scroll_delay=0.7,
        max_scroll_steps=max(3, min(30, (limit // 8) + 4)),
        remove_overlay_elements=True,
        simulate_user=True,
        magic=True,
    )
    try:
        async with AsyncWebCrawler(config=browser_config) as crawler:
            result = await crawler.arun(url=search_url, config=run_config)
            if not result.success:
                return [], [{"source": "browser_search", "url": search_url, "reason": result.error_message}]
            html = result.html or ""
            if _cloudflare_blocked(html, 403):
                return [], [{"source": "browser_search", "url": search_url, "reason": "cloudflare_managed_challenge"}]
            _extract_card_context(html)
            urls = _extract_event_links(html)
            for event in _extract_json_ld_events(html, search_url):
                event_url = _first_string(event.get("source_url"), event.get("url"))
                if event_url:
                    clean = _clean_url(event_url)
                    _cached_events[clean] = event
                    if clean not in urls:
                        urls.append(clean)
            return urls[:limit], []
    except Exception as error:
        return [], [{"source": "browser_search", "url": search_url, "reason": str(error)}]


def _merge_events(base: Dict[str, Any], detail: Dict[str, Any]) -> Dict[str, Any]:
    merged = {**base, **{key: value for key, value in detail.items() if value not in (None, "", [], {})}}
    for key in ("location", "organizer", "offers", "internationalConferenceAlerts"):
        if isinstance(base.get(key), dict) or isinstance(detail.get(key), dict):
            merged[key] = {
                **(base.get(key) if isinstance(base.get(key), dict) else {}),
                **(detail.get(key) if isinstance(detail.get(key), dict) else {}),
            }
    return merged


def _map_ica_event_to_generic(raw_event: Dict[str, Any]) -> Optional[GenericMappedEventDict]:
    name = _first_string(raw_event.get("name"), raw_event.get("title"))
    if not name:
        return None

    source_url = _first_string(raw_event.get("url"), raw_event.get("source_url"), raw_event.get("sourceUrl"))
    slug = _parse_slug_metadata(source_url or "")
    location = raw_event.get("location") if isinstance(raw_event.get("location"), dict) else {}
    address = location.get("address") if isinstance(location.get("address"), dict) else {}
    organizer = raw_event.get("organizer") if isinstance(raw_event.get("organizer"), dict) else {}
    ica = (
        raw_event.get("internationalConferenceAlerts")
        if isinstance(raw_event.get("internationalConferenceAlerts"), dict)
        else {}
    )
    categories = raw_event.get("categories") if isinstance(raw_event.get("categories"), list) else []

    city = _first_string(
        address.get("addressLocality"),
        raw_event.get("city"),
        ica.get("city"),
        slug.get("city"),
        location.get("name"),
    )
    country = _first_string(
        address.get("addressCountry"),
        raw_event.get("country"),
        ica.get("country"),
    )
    latitude = _first_number(location.get("latitude"), raw_event.get("latitude"))
    longitude = _first_number(location.get("longitude"), raw_event.get("longitude"))
    venue_name = _first_string(location.get("name"))
    category = _first_string(*(categories or []), raw_event.get("eventType"))
    organizer_contact = _clean_ica_field(raw_event.get("organizerContact"), 240)
    organizer_name = _clean_ica_organizer_name(_first_string(raw_event.get("organizerName"), organizer.get("name")))
    organizer_email = _clean_ica_email(organizer.get("email"))
    organizer_phone = _clean_ica_phone(organizer.get("telephone"))
    if not organizer_contact:
        organizer_contact = _first_string(
            " | ".join(part for part in (organizer_email, organizer_phone) if part)
        )
    people: List[Dict[str, Any]] = []
    for key, fallback_relationship in (
        ("speakers", "SPEAKER"),
        ("committeeMembers", "COMMITTEE MEMBER"),
    ):
        raw_people = raw_event.get(key) if isinstance(raw_event.get(key), list) else []
        for person in raw_people:
            if not isinstance(person, dict):
                continue
            person_name = _first_string(person.get("name"), person.get("fullName"))
            if not person_name:
                continue
            people.append(
                {
                    "fullName": person_name,
                    "title": _first_string(person.get("title")),
                    "relationshipType": _clean_ica_relationship_type(person.get("relationshipType"), fallback_relationship),
                    "metadataJson": {
                        "sourceProvider": "international_conference_alerts",
                        "person": person,
                    },
                }
            )

    occurrence: GenericOccurrenceDict = {
        "locationText": _first_string(venue_name, address.get("streetAddress"), city),
        "latitude": latitude,
        "longitude": longitude,
        "venueName": venue_name,
        "streetAddress": _first_string(address.get("streetAddress")),
        "city": city,
        "region": _first_string(address.get("addressRegion")),
        "postalCode": _first_string(address.get("postalCode")),
        "country": country,
        "timezone": _first_string(raw_event.get("timezone")),
        "expectedAttendance": _first_int(raw_event.get("expectedAttendance")),
    }
    occurrence = {key: value for key, value in occurrence.items() if value not in (None, "", [], {})}  # type: ignore

    metadata = _sanitize_ica_metadata(raw_event)

    event: GenericMappedEventDict = {
        "name": name,
        "sourceUrl": source_url,
        "startAt": _first_string(raw_event.get("startDate")),
        "endAt": _first_string(raw_event.get("endDate")),
        "city": city,
        "country": country,
        "eventType": _first_string(raw_event.get("eventType"), category, "Conference"),
        "category": category,
        "organizerName": organizer_name,
        "organizerWebsite": _first_string(raw_event.get("organizerWebsite"), organizer.get("url")),
        "organizerContact": organizer_contact,
        "eventImageUrl": _first_string(raw_event.get("eventImageUrl"), _first_url_or_string(raw_event.get("image"))),
        "industry": _first_string(raw_event.get("industry"), category),
        "expectedAttendance": _first_int(raw_event.get("expectedAttendance")),
        "description": _strip_html(raw_event.get("description")) or _first_string(raw_event.get("description")),
        "sourceProvider": "international_conference_alerts",
        "attendees": people,
        "occurrence": occurrence,
        "metadataJson": metadata,
    }
    return {key: value for key, value in event.items() if value not in (None, "", [], {})}  # type: ignore


def _clean_spaces(value: Any) -> Optional[str]:
    if value is None:
        return None
    text = re.sub(r"\s+", " ", str(value)).strip()
    return text or None


_ICA_NOISE_BOUNDARY_PATTERN = re.compile(
    r"\b(?:Find What Next|Similar Events|Explore Related Pages|Browse by|Subscribe to Event|Subscribe Now|Add to Calendar|Official Website|Top Countries|Top Cities|Popular Topics|Company About Us|Privacy Policy|Terms of Service|All rights reserved)\b",
    re.IGNORECASE,
)


def _clean_ica_field(value: Any, max_length: Optional[int] = None) -> Optional[str]:
    text = _clean_spaces(value)
    if not text:
        return None
    text = _ICA_NOISE_BOUNDARY_PATTERN.split(text, maxsplit=1)[0].strip(" ,|-")
    if max_length and len(text) > max_length:
        return None
    return text or None


def _clean_ica_email(value: Any) -> Optional[str]:
    text = _clean_ica_field(value, 200)
    if not text:
        return None
    match = re.search(r"[\w.+-]+@[\w.-]+\.[A-Za-z]{2,}", text)
    if match:
        return match.group(0)
    if "[email protected]" in text.lower():
        return "[email protected]"
    return None


def _clean_ica_phone(value: Any) -> Optional[str]:
    text = _clean_ica_field(value, 80)
    if not text:
        return None
    if not re.search(r"\d", text):
        return None
    return text


def _clean_ica_organizer_name(value: Any) -> Optional[str]:
    text = _clean_ica_field(value, 160)
    if not text:
        return None
    has_email = bool(re.search(r"[\w.+-]+@[\w.-]+\.[A-Za-z]{2,}", text) or "[email protected]" in text.lower())
    text_without_email = re.sub(r"[\w.+-]+@[\w.-]+\.[A-Za-z]{2,}", "", text)
    text_without_email = re.sub(r"\[email protected\]", "", text_without_email, flags=re.IGNORECASE)
    text_without_email = _clean_spaces(text_without_email) or text
    first_word = text_without_email.split(" ", 1)[0]
    if has_email and first_word.isupper() and 2 <= len(first_word) <= 12:
        return first_word
    return text_without_email


def _clean_ica_relationship_type(value: Any, fallback: str) -> str:
    relationship = _first_string(value, fallback) or fallback
    return relationship.replace("_", " ")


def _clean_ica_month(value: Any) -> Optional[str]:
    text = _clean_ica_field(value, 40)
    if not text:
        return None
    yyyymm_match = re.search(r"\b(20\d{2})(0[1-9]|1[0-2])\b", text)
    if yyyymm_match:
        return f"{yyyymm_match.group(1)}-{yyyymm_match.group(2)}"
    month_match = re.search(
        r"\b(January|February|March|April|May|June|July|August|September|October|November|December)(?:[-\s,]+(20\d{2}))?\b",
        text,
        re.IGNORECASE,
    )
    if month_match:
        month_name = month_match.group(1).title()
        year = month_match.group(2)
        return f"{month_name} {year}" if year else month_name
    return text


def _sanitize_ica_metadata(raw_event: Dict[str, Any]) -> Dict[str, Any]:
    metadata = {
        **raw_event,
        "sourceProvider": "international_conference_alerts",
    }
    organizer = metadata.get("organizer") if isinstance(metadata.get("organizer"), dict) else None
    if organizer is not None:
        clean_organizer = {**organizer}
        clean_organizer["name"] = _clean_ica_organizer_name(clean_organizer.get("name"))
        clean_organizer["email"] = _clean_ica_email(clean_organizer.get("email"))
        clean_organizer["telephone"] = _clean_ica_phone(clean_organizer.get("telephone"))
        metadata["organizer"] = {key: value for key, value in clean_organizer.items() if value not in (None, "", [], {})}

    ica = metadata.get("internationalConferenceAlerts")
    if isinstance(ica, dict):
        clean_ica = {**ica}
        clean_ica["month"] = _clean_ica_month(clean_ica.get("month"))
        quick_info = clean_ica.get("quickInfo")
        if isinstance(quick_info, dict):
            clean_quick_info = {**quick_info}
            clean_quick_info["month"] = _clean_ica_month(clean_quick_info.get("month"))
            clean_quick_info["venue"] = _clean_ica_field(clean_quick_info.get("venue"), 300)
            clean_ica["quickInfo"] = {
                key: value for key, value in clean_quick_info.items() if value not in (None, "", [], {})
            }
        metadata["internationalConferenceAlerts"] = {
            key: value for key, value in clean_ica.items() if value not in (None, "", [], {})
        }
    return metadata


def _meta_content(soup: BeautifulSoup, *selectors: Tuple[str, str]) -> Optional[str]:
    for attr, value in selectors:
        tag = soup.find("meta", attrs={attr: value})
        content = tag.get("content") if tag else None
        text = _clean_spaces(content)
        if text:
            return text
    return None


def _text_after_label(text: str, label: str) -> Optional[str]:
    pattern = re.compile(
        rf"{re.escape(label)}\s*:?\s*(.+?)(?=\s+(?:Start Date|End Date|Registration Deadline|Submission Deadline|Organizer Details|Organized By|Organizer|Contact Person|Event Enquiries|Phone|Call|Conference Agenda|Committee Members|Advisory Committee|Speakers|Indexed In|Quick Info|Type|Country|Month|Venue|Find What Next|Similar Events|Explore Related Pages|Browse by|Subscribe|Official Website)\b|$)",
        re.IGNORECASE,
    )
    match = pattern.search(text)
    return _clean_ica_field(match.group(1)) if match else None


def _collect_people_after_heading(soup: BeautifulSoup, heading_pattern: str, relationship_type: str) -> List[Dict[str, Any]]:
    heading = soup.find(
        ["h1", "h2", "h3", "h4", "h5"],
        string=lambda value: bool(value and re.search(heading_pattern, value, re.IGNORECASE)),
    )
    if not heading:
        return []

    people: List[Dict[str, Any]] = []
    for node in heading.find_all_next(["h2", "h3", "h4", "h5"]):
        title = _clean_spaces(node.get_text(" ", strip=True))
        if not title:
            continue
        if re.search(
            r"^(Organizer|Organizer Details|Indexed In|Quick Info|Conference Agenda|Important Dates|Venue|Objective of the Conference|Speakers?|Committee Members?|Advisory Committee)$",
            title,
            re.IGNORECASE,
        ):
            break
        detail_parts: List[str] = []
        sibling = node.find_next_sibling()
        while sibling and len(detail_parts) < 4:
            if getattr(sibling, "name", None) in {"h1", "h2", "h3", "h4", "h5"}:
                break
            sibling_text = _clean_spaces(sibling.get_text(" ", strip=True))
            if sibling_text:
                detail_parts.append(sibling_text)
            sibling = sibling.find_next_sibling()
        detail_text = _clean_spaces(" ".join(detail_parts)) or ""
        if title.lower() in {"speakers", "committee members"}:
            continue
        people.append(
            {
                "name": title,
                "title": detail_text[:300] or None,
                "relationshipType": relationship_type,
            }
        )
        if len(people) >= 50:
            break
    return people


def _extract_agenda_rows(soup: BeautifulSoup) -> List[Dict[str, str]]:
    rows: List[Dict[str, str]] = []
    for table in soup.find_all("table"):
        headers = [
            _clean_spaces(cell.get_text(" ", strip=True)) or ""
            for cell in table.find_all("th")
        ]
        header_text = " ".join(headers).lower()
        if headers and not ("timing" in header_text or "session" in header_text):
            continue
        for tr in table.find_all("tr"):
            cells = [
                _clean_spaces(cell.get_text(" ", strip=True)) or ""
                for cell in tr.find_all(["td", "th"])
            ]
            if len(cells) < 2 or cells[0].lower() == "timing":
                continue
            rows.append({"time": cells[0], "session": cells[1]})
    return rows


def _extract_ica_detail_html_event(html: str, source_url: str) -> Optional[Dict[str, Any]]:
    soup = BeautifulSoup(html or "", "html.parser")
    clean_source_url = _clean_url(source_url)
    slug = _parse_slug_metadata(clean_source_url)
    text = _clean_spaces(soup.get_text(" ", strip=True)) or ""
    if not text or "just a moment" in text.lower():
        return None

    raw_title = _first_string(
        _meta_content(soup, ("property", "og:title"), ("name", "twitter:title")),
        soup.find("h1").get_text(" ", strip=True) if soup.find("h1") else None,
        soup.title.get_text(" ", strip=True) if soup.title else None,
    )
    title = re.sub(r"\s*(?:\||-)\s*International Conference Alerts.*$", "", raw_title or "").strip()
    description = _first_string(
        _meta_content(soup, ("name", "description"), ("property", "og:description")),
        _text_after_label(text, "Objective of the Conference"),
    )

    start_date = _first_string(_text_after_label(text, "Start Date"))
    end_date = _first_string(_text_after_label(text, "End Date"))
    registration_deadline = _first_string(_text_after_label(text, "Registration Deadline"))
    submission_deadline = _first_string(_text_after_label(text, "Submission Deadline"))
    event_type = _first_string(_text_after_label(text, "Type"), "Conference")
    country = _first_string(_text_after_label(text, "Country"), slug.get("country"))
    month = _clean_ica_month(_text_after_label(text, "Month")) or _clean_ica_month(slug.get("month"))
    venue_text = _clean_ica_field(_text_after_label(text, "Venue"), 300)

    organizer_name = _clean_ica_organizer_name(
        _first_string(
            _text_after_label(text, "Organized By"),
            _text_after_label(text, "Organizer"),
        )
    )
    contact_person = _clean_ica_field(_text_after_label(text, "Contact Person"), 120)
    organizer_email = _clean_ica_email(_text_after_label(text, "Event Enquiries"))
    organizer_phone = _clean_ica_phone(_first_string(_text_after_label(text, "Phone"), _text_after_label(text, "Call")))
    organizer_contact = _clean_ica_field(
        " | ".join(part for part in (contact_person, organizer_email, organizer_phone) if part),
        240,
    )

    if not organizer_name:
        organizer_name = _clean_ica_organizer_name(_text_after_label(text, "Organized By"))

    location = _normalize_location({"name": venue_text or slug.get("city")})
    if slug.get("city"):
        location["address"] = {
            **(location.get("address") if isinstance(location.get("address"), dict) else {}),
            "addressLocality": slug.get("city"),
        }
    if country:
        location["address"] = {
            **(location.get("address") if isinstance(location.get("address"), dict) else {}),
            "addressCountry": country,
        }

    speakers = _collect_people_after_heading(soup, r"\bSpeakers?\b", "SPEAKER")
    committee_members = _collect_people_after_heading(soup, r"\bCommittee Members?\b|Advisory Committee", "COMMITTEE MEMBER")
    agenda = _extract_agenda_rows(soup)

    event = {
        "@type": "Event",
        "id": slug.get("id") or clean_source_url,
        "name": title or slug.get("acronym") or clean_source_url,
        "title": title or slug.get("acronym") or clean_source_url,
        "url": clean_source_url,
        "source_url": clean_source_url,
        "startDate": start_date,
        "endDate": end_date,
        "description": description,
        "location": location,
        "organizer": {
            "@type": "Organization",
            "name": organizer_name,
            "email": organizer_email,
            "telephone": organizer_phone,
        },
        "organizerName": organizer_name,
        "organizerContact": organizer_contact,
        "country": country,
        "eventType": event_type,
        "speakers": speakers,
        "committeeMembers": committee_members,
        "internationalConferenceAlerts": {
            "source": "detail_html_sections",
            "slug": slug.get("slug"),
            "acronym": slug.get("acronym"),
            "month": month,
            "importantDates": {
                "registrationDeadline": registration_deadline,
                "submissionDeadline": submission_deadline,
            },
            "agenda": agenda,
            "quickInfo": {
                "type": event_type,
                "country": country,
                "month": month,
                "venue": venue_text,
            },
            "contactPerson": contact_person,
        },
    }
    return {key: value for key, value in event.items() if value not in (None, "", [], {})}


def _map_ica_events_to_generic(events: List[Dict[str, Any]]) -> List[GenericMappedEventDict]:
    return [mapped for event in events if (mapped := _map_ica_event_to_generic(event))]


async def _fetch_detail_event_browser(url: str, proxy_url: Optional[str] = None) -> Tuple[Optional[Dict[str, Any]], Optional[Dict[str, Any]]]:
    clean_url = _clean_url(url)
    cached = _cached_events.get(clean_url)
    slug_fallback = cached or _compact_slug_event(clean_url)
    run_config = CrawlerRunConfig(
        cache_mode=CacheMode.BYPASS,
        wait_until="networkidle",
        delay_before_return_html=3.0,
        page_timeout=90000,
        scan_full_page=True,
        scroll_delay=0.5,
        max_scroll_steps=4,
        remove_overlay_elements=True,
        simulate_user=True,
        magic=True,
    )
    try:
        async with AsyncWebCrawler(config=_browser_config(proxy_url)) as crawler:
            result = await crawler.arun(url=clean_url, config=run_config)
            if not result.success:
                return slug_fallback, {"source": "browser_detail_fetch", "url": clean_url, "reason": result.error_message}
            html = result.html or ""
            if _cloudflare_blocked(html, 403):
                return slug_fallback, {"source": "browser_detail_fetch", "url": clean_url, "reason": "cloudflare_managed_challenge"}
            extracted = _extract_json_ld_events(html, clean_url)
            section_event = _extract_ica_detail_html_event(html, clean_url)
            if extracted:
                detail = _merge_events(extracted[0], section_event) if section_event else extracted[0]
                return (_merge_events(cached, detail) if cached else detail), None
            fallback = _extract_meta_fallback_event(html, clean_url)
            if fallback:
                detail = _merge_events(fallback, section_event) if section_event else fallback
                return (_merge_events(cached, detail) if cached else detail), None
            if section_event:
                return (_merge_events(cached, section_event) if cached else section_event), None
            return slug_fallback, {"source": "browser_detail_parse", "url": clean_url, "reason": "no_json_ld_or_meta_event_found"}
    except Exception as error:
        return slug_fallback, {"source": "browser_detail_fetch", "url": clean_url, "reason": str(error)}


async def _fetch_detail_event(client: httpx.AsyncClient, url: str, proxy_url: Optional[str] = None) -> Tuple[Optional[Dict[str, Any]], Optional[Dict[str, Any]]]:
    clean_url = _clean_url(url)
    cached = _cached_events.get(clean_url)
    slug_fallback = cached or _compact_slug_event(clean_url)
    try:
        response = await client.get(clean_url, headers=_headers(clean_url), timeout=30)
        unlocked = False
        if _cloudflare_blocked(response.text, response.status_code):
            unlocked_html, unlock_failure = await _fetch_via_brightdata(
                client,
                str(response.url),
                source="brightdata_detail_fetch",
            )
            if unlock_failure:
                event, failure = await _fetch_detail_event_browser(clean_url, proxy_url)
                if failure:
                    failure = {
                        **failure,
                        "static_status": response.status_code,
                        "static_reason": "cloudflare_managed_challenge",
                        "unlocker": unlock_failure,
                    }
                return event, failure
            html = unlocked_html or ""
            unlocked = True
        else:
            html = response.text
        if not unlocked:
            response.raise_for_status()
    except Exception as error:
        unlocked_html, unlock_failure = await _fetch_via_brightdata(
            client,
            clean_url,
            source="brightdata_detail_fetch",
        )
        if unlock_failure:
            event, failure = await _fetch_detail_event_browser(clean_url, proxy_url)
            if failure:
                failure = {
                    **failure,
                    "static_reason": str(error),
                    "unlocker": unlock_failure,
                }
            return event, failure
        html = unlocked_html or ""

    extracted = _extract_json_ld_events(html, clean_url)
    section_event = _extract_ica_detail_html_event(html, clean_url)
    if extracted:
        detail = _merge_events(extracted[0], section_event) if section_event else extracted[0]
        return (_merge_events(cached, detail) if cached else detail), None
    fallback = _extract_meta_fallback_event(html, clean_url)
    if fallback:
        detail = _merge_events(fallback, section_event) if section_event else fallback
        return (_merge_events(cached, detail) if cached else detail), None
    if section_event:
        return (_merge_events(cached, section_event) if cached else section_event), None
    return slug_fallback, {"source": "detail_parse", "url": clean_url, "reason": "no_json_ld_or_meta_event_found"}


def _event_location_query(event: Dict[str, Any]) -> Optional[str]:
    location = event.get("location") if isinstance(event.get("location"), dict) else {}
    address = location.get("address") if isinstance(location.get("address"), dict) else {}
    parts = [
        location.get("name"),
        address.get("streetAddress"),
        address.get("addressLocality"),
        address.get("addressRegion"),
        address.get("postalCode"),
        address.get("addressCountry"),
        event.get("country"),
    ]
    text = ", ".join(str(part).strip() for part in parts if str(part or "").strip())
    return text or None


async def _geocode_with_backend(client: httpx.AsyncClient, query: str) -> Optional[Tuple[float, float]]:
    geocoding_url = os.getenv("EAGLE_GEOCODING_URL", DEFAULT_GEOCODING_URL)
    try:
        response = await client.get(geocoding_url, params={"q": query}, timeout=20)
        response.raise_for_status()
        payload = response.json()
        data = payload.get("data") if isinstance(payload, dict) else payload
        if isinstance(data, dict) and isinstance(data.get("data"), dict):
            data = data["data"]
        lat = _first_number(
            data.get("latitude") if isinstance(data, dict) else None,
            data.get("lat") if isinstance(data, dict) else None,
        )
        lng = _first_number(
            data.get("longitude") if isinstance(data, dict) else None,
            data.get("lng") if isinstance(data, dict) else None,
        )
        if lat is not None and lng is not None:
            return lat, lng
    except Exception as error:
        logger.warning("Backend geocode failed query=%s error=%s", query, error)
    return None


async def _geocode_with_opencage(client: httpx.AsyncClient, query: str) -> Optional[Tuple[float, float]]:
    api_key = os.getenv("OPENCAGE_API_KEY")
    if not api_key:
        return None
    try:
        response = await client.get(
            "https://api.opencagedata.com/geocode/v1/json",
            params={"q": query, "key": api_key, "limit": 1, "no_annotations": 1},
            timeout=20,
        )
        response.raise_for_status()
        payload = response.json()
        result = (payload.get("results") or [{}])[0]
        geometry = result.get("geometry") if isinstance(result, dict) else {}
        lat = _first_number(geometry.get("lat") if isinstance(geometry, dict) else None)
        lng = _first_number(geometry.get("lng") if isinstance(geometry, dict) else None)
        if lat is not None and lng is not None:
            return lat, lng
    except Exception as error:
        logger.warning("OpenCage geocode failed query=%s error=%s", query, error)
    return None


async def _ensure_event_coordinates(
    client: httpx.AsyncClient,
    event: Dict[str, Any],
    geocode: bool,
    cache: Dict[str, Optional[Tuple[float, float]]],
) -> Optional[Tuple[float, float]]:
    location = event.get("location") if isinstance(event.get("location"), dict) else {}
    lat = _first_number(location.get("latitude"), event.get("latitude"))
    lng = _first_number(location.get("longitude"), event.get("longitude"))
    if lat is not None and lng is not None:
        return lat, lng
    if not geocode:
        return None
    query = _event_location_query(event)
    if not query:
        return None
    if query not in cache:
        cache[query] = await _geocode_with_backend(client, query) or await _geocode_with_opencage(client, query)
    coords = cache[query]
    if coords:
        location["latitude"], location["longitude"] = coords
        event["location"] = location
    return coords


def _haversine_km(lat1: float, lng1: float, lat2: float, lng2: float) -> float:
    radius = 6371.0088
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    delta_phi = math.radians(lat2 - lat1)
    delta_lam = math.radians(lng2 - lng1)
    a = math.sin(delta_phi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(delta_lam / 2) ** 2
    return 2 * radius * math.atan2(math.sqrt(a), math.sqrt(1 - a))


async def _filter_by_radius(
    events: List[Dict[str, Any]],
    *,
    lat: Optional[float],
    lng: Optional[float],
    radius_km: Optional[float],
    geocode: bool,
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]], Dict[str, Any]]:
    if lat is None or lng is None or radius_km is None:
        return events, [], {"enabled": False}

    kept: List[Dict[str, Any]] = []
    failures: List[Dict[str, Any]] = []
    geocode_cache: Dict[str, Optional[Tuple[float, float]]] = {}
    async with httpx.AsyncClient(follow_redirects=True) as client:
        for event in events:
            coords = await _ensure_event_coordinates(client, event, geocode, geocode_cache)
            if not coords:
                failures.append({"source": "radius_filter", "event": event.get("name"), "url": event.get("source_url"), "reason": "missing_coordinates"})
                continue
            distance = _haversine_km(lat, lng, coords[0], coords[1])
            event["distance_km"] = round(distance, 3)
            if distance <= radius_km:
                kept.append(event)
    return kept, failures, {"enabled": True, "before": len(events), "after": len(kept), "radius_km": radius_km, "lat": lat, "lng": lng, "geocode_cache_size": len(geocode_cache)}


async def crawl_ica_events_with_diagnostics(
    *,
    search_url: Optional[str] = None,
    q: Optional[str] = None,
    country: Optional[str] = None,
    month: Optional[str] = None,
    topic_slug: Optional[str] = None,
    subtopic_slug: Optional[str] = None,
    city_slug: Optional[str] = None,
    page: int = 1,
    limit: int = 50,
    source: ICA_SOURCE = "auto",
    enrich_details: bool = True,
    lat: Optional[float] = None,
    lng: Optional[float] = None,
    radius_km: Optional[float] = None,
    geocode: bool = True,
    proxy_url: Optional[str] = None,
) -> Dict[str, Any]:
    if source not in ("auto", "api", "html", "sitemap"):
        source = "auto"

    resolved_url = _build_search_url(
        search_url=search_url,
        q=q,
        country=country,
        month=month,
        topic_slug=topic_slug,
        subtopic_slug=subtopic_slug,
        city_slug=city_slug,
        page=page,
    )
    params = {
        "q": q,
        "country": country,
        "month": month,
        "page": page,
        "topic": topic_slug,
        "subtopic": subtopic_slug,
        "city": city_slug,
        **{key: values[0] for key, values in parse_qs(urlparse(resolved_url).query).items() if values},
    }
    effective_q = params.get("q") if isinstance(params.get("q"), str) else q
    effective_country = params.get("country") if isinstance(params.get("country"), str) else country
    effective_month = params.get("month") if isinstance(params.get("month"), str) else month
    parse_failures: List[Dict[str, Any]] = []
    events: List[Dict[str, Any]] = []
    urls: List[str] = []
    diagnostics: Dict[str, Any] = {
        "search_url": resolved_url,
        "source": source,
        "proxy_enabled": bool(_resolve_proxy_url(proxy_url)),
    }

    async with httpx.AsyncClient(timeout=45, follow_redirects=True) as client:
        if source in ("auto", "api"):
            api_events, api_failures = await _probe_api_candidates(client=client, params=params, limit=limit)
            parse_failures.extend(api_failures)
            for event in api_events:
                event_url = _first_string(event.get("source_url"), event.get("url"))
                if event_url:
                    _cached_events[_clean_url(event_url)] = event
                events.append(event)
            diagnostics["api_event_count"] = len(api_events)
            if source == "api":
                filtered, radius_failures, radius_diag = await _filter_by_radius(
                    events[:limit],
                    lat=lat,
                    lng=lng,
                    radius_km=radius_km,
                    geocode=geocode,
                )
                return {
                    "events": _map_ica_events_to_generic(filtered),
                    "parse_failures": [*parse_failures, *radius_failures],
                    "diagnostics": {**diagnostics, "radius": radius_diag},
                }

        if source in ("auto", "html") and len(events) < limit:
            html_urls, html_failures = await _search_html_urls(client=client, search_url=resolved_url, limit=limit)
            parse_failures.extend(html_failures)
            urls.extend(url for url in html_urls if url not in urls)

    if source in ("auto", "html") and len(events) + len(urls) < limit:
        browser_urls, browser_failures = await _search_browser_urls(resolved_url, limit, proxy_url)
        parse_failures.extend(browser_failures)
        urls.extend(url for url in browser_urls if url not in urls)

    if source in ("auto", "sitemap") and len(events) + len(urls) < limit:
        async with httpx.AsyncClient(timeout=45, follow_redirects=True) as client:
            sitemap_urls, sitemap_failures = await _fetch_sitemap_urls(
                client=client,
                q=effective_q,
                country=effective_country,
                month=effective_month,
                limit=limit,
            )
            parse_failures.extend(sitemap_failures)
            urls.extend(url for url in sitemap_urls if url not in urls)

    diagnostics["candidate_url_count"] = len(urls)

    if enrich_details and urls:
        semaphore = asyncio.Semaphore(4)
        detail_events: List[Dict[str, Any]] = []

        async with httpx.AsyncClient(timeout=45, follow_redirects=True) as client:
            async def crawl_one(url: str) -> None:
                async with semaphore:
                    event, failure = await _fetch_detail_event(client, url, proxy_url)
                    if event:
                        detail_events.append(event)
                    if failure:
                        parse_failures.append(failure)

            await asyncio.gather(*(crawl_one(url) for url in urls[: max(limit, 1)]))
        events.extend(detail_events)
    else:
        for url in urls:
            cached = _cached_events.get(_clean_url(url))
            events.append(cached or _compact_slug_event(url))

    deduped: Dict[str, Dict[str, Any]] = {}
    for event in events:
        key = str(event.get("source_url") or event.get("url") or event.get("id") or len(deduped))
        deduped[key] = event

    filtered, radius_failures, radius_diag = await _filter_by_radius(
        list(deduped.values())[:limit],
        lat=lat,
        lng=lng,
        radius_km=radius_km,
        geocode=geocode,
    )
    parse_failures.extend(radius_failures)
    diagnostics["radius"] = radius_diag
    diagnostics["final_count_before_radius"] = len(deduped)
    return {"events": _map_ica_events_to_generic(filtered[:limit]), "parse_failures": parse_failures, "diagnostics": diagnostics}


async def preview_ica_events(
    *,
    events: List[Dict[str, Any]],
    parse_failures: Optional[List[Dict[str, Any]]] = None,
    diagnostics: Optional[Dict[str, Any]] = None,
    persist: bool = False,
) -> Dict[str, Any]:
    parse_failures = parse_failures or []
    diagnostics = diagnostics or {}
    if persist:
        diagnostics["persist_warning"] = (
            "InternationalConferenceAlerts backend import is not wired yet; returning preview only."
        )

    return {
        "mode": "preview",
        "eagle_ingest_url": os.getenv("EAGLE_API_BASE_URL", DEFAULT_EAGLE_API_BASE_URL).rstrip("/"),
        "eagle_endpoint_url": "",
        "crawled_count": len(events),
        "normalized_count": len(events),
        "ingested_count": 0,
        "created_count": 0,
        "updated_count": 0,
        "skipped_count": 0,
        "failed_count": len(parse_failures),
        "events": events,
        "results": [],
        "failures": [],
        "parse_failures": parse_failures,
        "diagnostics": diagnostics,
    }


def _sanitize_ica_event_for_eagle(event: Dict[str, Any]) -> Dict[str, Any]:
    payload = dict(event)
    description = payload.get("description")
    if isinstance(description, str) and len(description) > 5000:
        payload["description"] = description[:5000]
    priority_explanation = payload.get("priorityExplanation") or payload.get("priority_explanation")
    if isinstance(priority_explanation, str) and len(priority_explanation) > 5000:
        payload["priorityExplanation"] = priority_explanation[:5000]
    return payload


async def ingest_ica_events_to_eagle(
    *,
    organization_id: Optional[str],
    workspace_id: Optional[str],
    events: List[Dict[str, Any]],
    parse_failures: Optional[List[Dict[str, Any]]] = None,
    diagnostics: Optional[Dict[str, Any]] = None,
    persist: bool = False,
) -> Dict[str, Any]:
    response = await ingest_generic_events_to_eagle(
        organization_id=organization_id,
        workspace_id=workspace_id,
        events=events,
        source_provider="international_conference_alerts",
        parse_failures=parse_failures,
        persist=persist,
        already_mapped=True,
    )
    if diagnostics:
        response["diagnostics"] = diagnostics
    return response
