import asyncio
import json
import logging
import math
import os
import re
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Literal, Optional, Tuple
from urllib.parse import quote, urljoin, urlparse

os.environ.setdefault(
    "CRAWL4_AI_BASE_DIRECTORY",
    str(Path(__file__).resolve().parent / ".crawl4ai_data"),
)

import httpx
from bs4 import BeautifulSoup
from crawl4ai import AsyncWebCrawler, BrowserConfig, CacheMode, CrawlerRunConfig
from ddgs import DDGS

logger = logging.getLogger(__name__)

DEFAULT_EAGLE_API_BASE_URL = "http://localhost:3001/api/v1"
DEFAULT_EAGLE_IMPORT_BATCH_SIZE = 20
HUMANITIX_EVENT_URL_PATTERN = re.compile(
    r"https?://events\.humanitix\.com/[^\s\"'<>]+",
    re.IGNORECASE,
)
HUMANITIX_SEARCH_API_URL = "https://humanitix.com/api/search"
HumanitixSource = Literal["auto", "api", "html"]
HUMANITIX_NON_EVENT_PATH_SEGMENTS = {
    "tickets",
    "waitlist",
    "checkout",
    "register",
    "orders",
}
HUMANITIX_LOCATION_SLUGS = {
    "sydney": "au--nsw--sydney",
    "melbourne": "au--vic--melbourne",
    "brisbane": "au--qld--brisbane",
    "perth": "au--wa--perth",
    "adelaide": "au--sa--adelaide",
    "canberra": "au--act--canberra",
    "gold coast": "au--qld--gold-coast",
    "newcastle": "au--nsw--newcastle",
    "wollongong": "au--nsw--wollongong",
    "new york": "us--ny--new-york",
    "new york city": "us--ny--new-york",
    "new york, ny": "us--ny--new-york",
    "nyc": "us--ny--new-york",
}

_cached_events: Dict[str, List[Dict[str, Any]]] = {}


def _humanitix_headers(referer: Optional[str] = None) -> Dict[str, str]:
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


def _cache_humanitix_event(event: Dict[str, Any], urls: List[str]) -> None:
    event_url = _clean_url(event.get("url") or event.get("source_url") or "")
    if event_url and _is_humanitix_event_page(event_url):
        _cached_events.setdefault(event_url, []).append(event)
        if event_url not in urls:
            urls.append(event_url)


def _clean_url(url: str) -> str:
    parsed = urlparse(url)
    return f"{parsed.scheme}://{parsed.netloc}{parsed.path}"


def _event_id_from_url(url: str) -> Optional[str]:
    path = urlparse(url).path.strip("/")
    return path or None


def _is_humanitix_event_page(url: str) -> bool:
    parsed = urlparse(url)
    if parsed.netloc.lower() != "events.humanitix.com":
        return False
    parts = [part for part in parsed.path.strip("/").split("/") if part]
    if len(parts) != 1:
        return False
    return parts[0].lower() not in HUMANITIX_NON_EVENT_PATH_SEGMENTS


def _as_list(value: Any) -> List[Any]:
    if value is None:
        return []
    return value if isinstance(value, list) else [value]


def _json_loads_safe(value: str) -> Optional[Any]:
    try:
        return json.loads(value)
    except Exception:
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


def _parse_humanitix_date(value: Any) -> Optional[str]:
    if not isinstance(value, str) or not value.strip():
        return None

    text = value.strip()
    if text.endswith("Z") or "T" in text:
        return text

    cleaned = re.sub(r"\s*\(.+?\)\s*$", "", text)
    for fmt in (
        "%a %b %d %Y %H:%M:%S GMT%z",
        "%a %b %d %Y %H:%M:%S %z",
    ):
        try:
            return datetime.strptime(cleaned, fmt).isoformat()
        except ValueError:
            continue

    return text


def _humanitix_image_url(handle: Any, variant: str = "seo-500.jpg") -> Optional[str]:
    if not isinstance(handle, str) or not handle.strip():
        return None
    handle = handle.strip()
    if handle.startswith("http://") or handle.startswith("https://"):
        return handle
    return f"https://images.humanitix.com/i/{handle}@{variant}"


def _address_component_value(components: Any, component_type: str) -> Optional[str]:
    if not isinstance(components, list):
        return None

    for component in components:
        if not isinstance(component, dict):
            continue
        types = component.get("types") or []
        if component_type in types:
            return component.get("long_name") or component.get("short_name")

    return None


def _compact_humanitix_search_event(data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    slug = data.get("slug")
    if not isinstance(slug, str) or not slug:
        return None

    hostname = data.get("hostname") or "https://events.humanitix.com/"
    clean_source_url = _clean_url(urljoin(str(hostname), slug))
    date = data.get("date") if isinstance(data.get("date"), dict) else {}
    event_location = data.get("eventLocation") if isinstance(data.get("eventLocation"), dict) else {}
    address_components = event_location.get("addressComponents")
    organiser = data.get("organiser") if isinstance(data.get("organiser"), dict) else {}
    pricing = data.get("pricing") if isinstance(data.get("pricing"), dict) else {}
    banner_image = data.get("bannerImage") if isinstance(data.get("bannerImage"), dict) else {}

    start_date = _parse_humanitix_date(date.get("startDate"))
    end_date = _parse_humanitix_date(date.get("endDate"))
    event_id = data.get("_id") or _event_id_from_url(clean_source_url)
    if start_date:
        event_id = f"{event_id}:{start_date}"

    compact = {
        "@type": "Event",
        "id": event_id,
        "name": data.get("name"),
        "url": clean_source_url,
        "source_url": clean_source_url,
        "startDate": start_date,
        "endDate": end_date,
        "timezone": data.get("timezone"),
        "eventStatus": "https://schema.org/EventScheduled",
        "eventAttendanceMode": "https://schema.org/OfflineEventAttendanceMode",
        "image": _humanitix_image_url(banner_image.get("handle")),
        "description": data.get("name"),
        "location": {
            "@type": "Place",
            "name": event_location.get("venueName"),
            "address": {
                "@type": "PostalAddress",
                "streetAddress": event_location.get("address"),
                "addressLocality": _address_component_value(address_components, "locality"),
                "addressRegion": _address_component_value(address_components, "administrative_area_level_1"),
                "postalCode": _address_component_value(address_components, "postal_code"),
                "addressCountry": _address_component_value(address_components, "country"),
            },
        },
        "organizer": {
            "@type": "Organization",
            "name": organiser.get("name"),
        },
        "offers": {
            "@type": "AggregateOffer",
            "lowPrice": pricing.get("minimumPrice"),
            "highPrice": pricing.get("maximumPrice"),
            "priceCurrency": "AUD",
        },
        "humanitix": {
            "event_id": data.get("_id"),
            "slug": slug,
            "host_total_followers": data.get("hostTotalFollowers"),
            "organizer_id": organiser.get("_id"),
            "organizer_followers": organiser.get("followerCount"),
            "is_recurring": data.get("isRecurring"),
            "location": data.get("location"),
        },
        "extraction_source": "search_page_next_data",
    }

    return {key: value for key, value in compact.items() if value not in (None, "", [], {})}


def _extract_humanitix_initial_events(html: str) -> List[Dict[str, Any]]:
    page_props = _extract_humanitix_page_props(html)
    initial_events = page_props.get("initialEvents")
    if not isinstance(initial_events, list):
        return []

    events: List[Dict[str, Any]] = []
    for item in initial_events:
        if not isinstance(item, dict):
            continue
        event = _compact_humanitix_search_event(item)
        if event:
            events.append(event)

    return events


def _extract_humanitix_page_props(html: str) -> Dict[str, Any]:
    soup = BeautifulSoup(html or "", "html.parser")
    next_data = soup.find("script", id="__NEXT_DATA__")
    if not next_data:
        return {}

    payload = _json_loads_safe(next_data.string or next_data.get_text() or "")
    if not isinstance(payload, dict):
        return {}

    page_props = (
        payload.get("props", {}).get("pageProps", {})
        if isinstance(payload.get("props"), dict)
        else {}
    )
    return page_props if isinstance(page_props, dict) else {}


def _humanitix_search_api_payload(
    *,
    page_props: Dict[str, Any],
    keyword: str,
    page: int,
) -> Dict[str, Any]:
    query = page_props.get("query") if isinstance(page_props.get("query"), dict) else {}
    current_location = (
        page_props.get("currentLocation")
        if isinstance(page_props.get("currentLocation"), dict)
        else {}
    )
    parsed_categories = (
        page_props.get("parsedCategories")
        if isinstance(page_props.get("parsedCategories"), dict)
        else {}
    )

    search_terms = query.get("search")
    search_text = (
        " ".join(str(item) for item in search_terms if item)
        if isinstance(search_terms, list)
        else str(search_terms or keyword)
    ).strip()

    category = parsed_categories.get("category")
    subcategory = parsed_categories.get("subcategory")
    modifier = parsed_categories.get("modifier")

    payload: Dict[str, Any] = {
        "query": search_text or keyword.strip(),
        "locationQuery": "",
        "locationType": "",
        "types": [],
        "categories": [category] if category else [],
        "subcategories": [subcategory] if subcategory else [],
        "interests": [],
        "prices": "free" if modifier == "free" else "all",
        "dates": query.get("dates") or "all",
        "startDate": "",
        "endDate": "",
        "accessibility": [],
        "page": page,
        "safeSearch": page_props.get("safeSearch", True),
    }

    geocode_keys = ("name", "latLng", "northeast", "southwest", "area")
    geocode = {key: current_location.get(key) for key in geocode_keys if current_location.get(key) is not None}
    if geocode:
        payload["geocode"] = geocode

    return payload


async def _search_humanitix_api_events(
    *,
    client: httpx.AsyncClient,
    search_url: str,
    keyword: str,
    limit: int,
    page_props: Dict[str, Any],
    seed_count: int,
    seed_urls: Optional[set[str]] = None,
) -> Tuple[List[Dict[str, Any]], List[str]]:
    events: List[Dict[str, Any]] = []
    failures: List[str] = []
    seen_urls: set[str] = set(seed_urls or set())
    max_pages = min(max(3, math.ceil(max(limit, 1) / 16) + 4), 75)
    empty_unique_pages = 0

    for page in range(1, max_pages + 1):
        payload = _humanitix_search_api_payload(page_props=page_props, keyword=keyword, page=page)
        try:
            response = await client.post(
                HUMANITIX_SEARCH_API_URL,
                headers={
                    **_humanitix_headers(search_url),
                    "Accept": "application/json",
                    "Content-Type": "application/json",
                },
                json=payload,
            )
            response.raise_for_status()
            data = response.json()
        except Exception as error:
            failures.append(f"page={page}: {error}")
            break

        raw_events = data if isinstance(data, list) else data.get("events") if isinstance(data, dict) else []
        if not isinstance(raw_events, list) or not raw_events:
            break

        new_count = 0
        for item in raw_events:
            if not isinstance(item, dict):
                continue
            event = _compact_humanitix_search_event(item)
            if not event:
                continue
            event_url = _clean_url(event.get("url") or event.get("source_url") or "")
            if not event_url or event_url in seen_urls:
                continue
            seen_urls.add(event_url)
            events.append(event)
            new_count += 1

        unique_total = seed_count + len(events)
        if unique_total >= limit:
            break

        if new_count == 0:
            empty_unique_pages += 1
            if empty_unique_pages >= 2:
                break
        else:
            empty_unique_pages = 0

    return events, failures


def _extract_json_ld_events(html: str, source_url: str) -> List[Dict[str, Any]]:
    soup = BeautifulSoup(html or "", "html.parser")
    events: List[Dict[str, Any]] = []

    for script in soup.find_all("script", attrs={"type": "application/ld+json"}):
        payload = _json_loads_safe(script.string or script.get_text() or "")
        for item in _iter_json_ld_events(payload):
            events.append(_compact_humanitix_event(item, source_url))

    return events


def _extract_humanitix_detail_fallback(html: str, source_url: str) -> List[Dict[str, Any]]:
    soup = BeautifulSoup(html or "", "html.parser")
    title_tag = soup.find("meta", attrs={"property": "og:title"})
    url_tag = soup.find("meta", attrs={"property": "og:url"})
    image_tag = soup.find("meta", attrs={"property": "og:image"})
    description_tag = soup.find("meta", attrs={"name": "description"}) or soup.find(
        "meta",
        attrs={"property": "og:description"},
    )
    location_tag = soup.find("meta", attrs={"name": "twitter:data1"})
    date_tag = soup.find("meta", attrs={"name": "twitter:data2"})
    title_node = soup.select_one("[data-testid=title]") or soup.find("h1")

    name = (
        title_tag.get("content")
        if title_tag and title_tag.get("content")
        else title_node.get_text(" ", strip=True)
        if title_node
        else None
    )
    if not name:
        return []

    clean_source_url = _clean_url(
        url_tag.get("content") if url_tag and url_tag.get("content") else source_url
    )
    text = " ".join(soup.get_text(" ", strip=True).split())
    description = description_tag.get("content") if description_tag else None
    marker = "Description "
    if marker in text:
        detail_description = text.split(marker, 1)[1]
        detail_description = re.split(r"\s+(Get tickets|Refund Policy|Contact host)\b", detail_description, maxsplit=1)[0]
        if len(detail_description) > len(description or ""):
            description = detail_description

    compact = {
        "@type": "Event",
        "id": _event_id_from_url(clean_source_url),
        "name": name,
        "url": clean_source_url,
        "source_url": clean_source_url,
        "description": description,
        "image": image_tag.get("content") if image_tag else None,
        "location": {
            "@type": "Place",
            "address": {
                "@type": "PostalAddress",
                "streetAddress": location_tag.get("content") if location_tag else None,
            },
        },
        "humanitix": {
            "display_date": date_tag.get("content") if date_tag else None,
        },
        "extraction_source": "detail_meta_html",
    }

    return [{key: value for key, value in compact.items() if value not in (None, "", [], {})}]


def _extract_humanitix_event_links(html: str) -> List[str]:
    soup = BeautifulSoup(html or "", "html.parser")
    urls: List[str] = []

    for anchor in soup.find_all("a", href=True):
        href = anchor.get("href") or ""
        match = HUMANITIX_EVENT_URL_PATTERN.search(href)
        if not match:
            continue
        clean = _clean_url(match.group(0).replace("&amp;", "&"))
        if _is_humanitix_event_page(clean) and clean not in urls:
            urls.append(clean)

    if urls:
        return urls

    for match in HUMANITIX_EVENT_URL_PATTERN.finditer(html or ""):
        clean = _clean_url(match.group(0).replace("&amp;", "&"))
        if _is_humanitix_event_page(clean) and clean not in urls:
            urls.append(clean)

    return urls


def _compact_humanitix_event(data: Dict[str, Any], source_url: str) -> Dict[str, Any]:
    clean_source_url = _clean_url(data.get("url") or source_url)
    event_id = data.get("id") or _event_id_from_url(clean_source_url)
    if data.get("startDate"):
        event_id = f"{event_id}:{data.get('startDate')}"

    compact = {
        **data,
        "id": event_id,
        "source_url": clean_source_url,
        "url": clean_source_url,
        "extraction_source": "json_ld",
    }

    return {key: value for key, value in compact.items() if value not in (None, "", [], {})}


def _page_debug_snippet(html: str, markdown: Optional[str] = None) -> Dict[str, Any]:
    soup = BeautifulSoup(html or "", "html.parser")
    title = (soup.title.string or "").strip() if soup.title and soup.title.string else None
    text = " ".join(soup.get_text(" ", strip=True).split())
    if not text and markdown:
        text = " ".join(markdown.split())

    lower_text = text.lower()
    possible_block = any(
        marker in lower_text
        for marker in (
            "access denied",
            "captcha",
            "not a bot",
            "enable javascript",
            "blocked",
        )
    )

    return {
        "title": title,
        "snippet": text[:700],
        "possible_block_or_bot_page": possible_block,
    }


def _humanitix_location_slug(location: str) -> Optional[str]:
    normalized = location.strip().lower()
    if "--" in normalized:
        return normalized
    return HUMANITIX_LOCATION_SLUGS.get(normalized)


def _humanitix_directory_url(location: str) -> Optional[str]:
    slug = _humanitix_location_slug(location)
    if not slug:
        return None
    return f"https://humanitix.com/au/events/{slug}"


def _humanitix_search_url(location: str, keyword: str) -> Optional[str]:
    slug = _humanitix_location_slug(location)
    keyword = keyword.strip()
    if not slug or not keyword:
        return None
    return f"https://humanitix.com/au/search/{slug}/{quote(keyword)}?dates=all"


def _event_matches_keyword(event: Dict[str, Any], keyword: str) -> bool:
    keyword = keyword.strip().lower()
    if not keyword:
        return True
    text = " ".join(
        str(event.get(key) or "")
        for key in ("name", "description", "category", "eventStatus", "eventAttendanceMode")
    ).lower()
    return keyword in text


def _merge_humanitix_events(base: Dict[str, Any], detail: Dict[str, Any]) -> Dict[str, Any]:
    merged = {**base, **detail}

    for key in ("humanitix", "location", "organizer", "offers"):
        base_value = base.get(key)
        detail_value = detail.get(key)
        if isinstance(base_value, dict) and isinstance(detail_value, dict):
            merged[key] = {**base_value, **detail_value}

    if base.get("id") and not detail.get("id"):
        merged["id"] = base["id"]
    if base.get("startDate") and not detail.get("startDate"):
        merged["startDate"] = base["startDate"]
    if base.get("endDate") and not detail.get("endDate"):
        merged["endDate"] = base["endDate"]

    return {key: value for key, value in merged.items() if value not in (None, "", [], {})}


async def search_humanitix_urls(
    *,
    keyword: str,
    location: str,
    limit: int = 20,
    source: HumanitixSource = "auto",
) -> List[str]:
    urls: List[str] = []
    search_url = _humanitix_search_url(location, keyword)
    directory_url = _humanitix_directory_url(location)

    browser_config = BrowserConfig(
        headless=True,
        verbose=False,
        viewport_width=1920,
        viewport_height=1080,
    )
    run_config = CrawlerRunConfig(
        cache_mode=CacheMode.BYPASS,
        delay_before_return_html=2.0,
        page_timeout=45000,
    )

    if source not in ("auto", "api", "html"):
        source = "auto"

    if search_url:
        logger.info("Crawling Humanitix search: %s source=%s", search_url, source)

        try:
            async with httpx.AsyncClient(timeout=30, follow_redirects=True) as client:
                response = await client.get(search_url, headers=_humanitix_headers())
                response.raise_for_status()
                page_props = _extract_humanitix_page_props(response.text)

                html_events = _extract_humanitix_initial_events(response.text)
                for event in html_events:
                    _cache_humanitix_event(event, urls)

                if source in ("auto", "api") and len(urls) < limit:
                    api_events, api_failures = await _search_humanitix_api_events(
                        client=client,
                        search_url=search_url,
                        keyword=keyword,
                        limit=limit,
                        page_props=page_props,
                        seed_count=len(urls),
                        seed_urls=set(urls),
                    )
                    for event in api_events:
                        _cache_humanitix_event(event, urls)
                    if api_failures:
                        logger.warning("Humanitix search API failed: %s", "; ".join(api_failures))

                if source == "html" and len(urls) < limit:
                    for clean in _extract_humanitix_event_links(response.text):
                        if clean not in urls:
                            urls.append(clean)

                if urls:
                    return urls[:limit]
        except Exception as error:
            logger.exception("Humanitix search page crawl exception: %s", error)

    if directory_url and not urls and source in ("auto", "html"):
        pages_to_crawl = min(max(1, math.ceil(limit / 15)), 10)
        logger.info("Crawling Humanitix directory: %s pages=%s", directory_url, pages_to_crawl)

        try:
            async with AsyncWebCrawler(config=browser_config) as crawler:
                for page in range(1, pages_to_crawl + 1):
                    page_url = f"{directory_url}?page={page}"
                    result = await crawler.arun(url=page_url, config=run_config)
                    if not result.success:
                        logger.warning("Humanitix directory crawl failed url=%s error=%s", page_url, result.error_message)
                        break

                    html = result.html or ""
                    json_ld_events = _extract_json_ld_events(html, page_url)
                    found_structured_events = bool(json_ld_events)
                    for event in json_ld_events:
                        if not _event_matches_keyword(event, keyword):
                            continue
                        event_url = _clean_url(event.get("url") or event.get("source_url") or "")
                        if event_url and _is_humanitix_event_page(event_url):
                            _cached_events.setdefault(event_url, []).append(event)
                            if event_url not in urls:
                                urls.append(event_url)

                    # Humanitix directory pages expose proper schema.org Event JSON-LD.
                    # Prefer that structured list. Raw href scraping is only a fallback
                    # because it also includes /tickets, /waitlist, and repeated-date links.
                    if not found_structured_events:
                        for clean in _extract_humanitix_event_links(html):
                            if clean not in urls:
                                urls.append(clean)

                    if len(urls) >= limit:
                        return urls[:limit]
        except Exception as error:
            logger.exception("Humanitix directory crawl exception: %s", error)

    if not urls:
        logger.info("Falling back to search index for Humanitix keyword=%s location=%s", keyword, location)
        query = f"site:events.humanitix.com {keyword} {location}"
        try:
            with DDGS() as ddgs:
                for result in ddgs.text(query, max_results=limit):
                    href = result.get("href") or result.get("url") or ""
                    match = HUMANITIX_EVENT_URL_PATTERN.search(href)
                    if not match:
                        continue
                    clean = _clean_url(match.group(0))
                    if _is_humanitix_event_page(clean) and clean not in urls:
                        urls.append(clean)
                    if len(urls) >= limit:
                        break
        except Exception as error:
            logger.exception("Humanitix search fallback failed: %s", error)

    return urls[:limit]


async def crawl_humanitix_events_with_diagnostics(
    *,
    keyword: str,
    location: str,
    limit: int = 20,
    concurrency: int = 4,
    source: HumanitixSource = "auto",
) -> Dict[str, Any]:
    urls = await search_humanitix_urls(keyword=keyword, location=location, limit=limit, source=source)
    if not urls:
        return {
            "events": [],
            "parse_failures": [
                {
                    "reason": "no_humanitix_urls_found",
                    "keyword": keyword,
                    "location": location,
                }
            ],
        }

    browser_config = BrowserConfig(
        headless=True,
        verbose=False,
        viewport_width=1920,
        viewport_height=1080,
    )
    run_config = CrawlerRunConfig(
        cache_mode=CacheMode.BYPASS,
        delay_before_return_html=2.0,
        page_timeout=45000,
    )
    semaphore = asyncio.Semaphore(max(1, concurrency))
    events: List[Dict[str, Any]] = []
    parse_failures: List[Dict[str, Any]] = []

    async with AsyncWebCrawler(config=browser_config) as crawler:
        async def crawl_one(url: str) -> None:
            clean_url = _clean_url(url)
            async with semaphore:
                # URLs returned by search_humanitix_urls already come from a
                # keyword-filtered Humanitix search result. Do not text-match
                # again here: Humanitix returns valid "conference" results
                # whose title/description may say "summit", "forum", etc.
                cached = list(_cached_events.get(clean_url, []))

                logger.info("Browser-crawling Humanitix page: %s", clean_url)
                try:
                    detail_extracted: List[Dict[str, Any]] = []
                    try:
                        async with httpx.AsyncClient(timeout=30, follow_redirects=True) as client:
                            response = await client.get(
                                clean_url,
                                headers={
                                    "User-Agent": (
                                        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                                        "AppleWebKit/537.36 (KHTML, like Gecko) "
                                        "Chrome/126.0.0.0 Safari/537.36"
                                    ),
                                    "Accept-Language": "en-US,en;q=0.9",
                                },
                            )
                            response.raise_for_status()
                            detail_extracted = _extract_json_ld_events(response.text, clean_url)
                            if not detail_extracted:
                                detail_extracted = _extract_humanitix_detail_fallback(response.text, clean_url)
                    except Exception as detail_error:
                        logger.warning("Humanitix detail HTTP parse failed url=%s error=%s", clean_url, detail_error)

                    if detail_extracted:
                        if cached:
                            events.extend(_merge_humanitix_events(cached[0], event) for event in detail_extracted)
                        else:
                            events.extend(detail_extracted)
                        return

                    if cached:
                        events.extend(cached)
                        return

                    result = await crawler.arun(url=clean_url, config=run_config)
                    if not result.success:
                        parse_failures.append(
                            {
                                "url": clean_url,
                                "reason": "crawl_failed",
                                "error": result.error_message,
                            }
                        )
                        return

                    extracted = _extract_json_ld_events(result.html or "", clean_url)
                    if extracted:
                        events.extend(extracted)
                    else:
                        parse_failures.append(
                            {
                                "url": clean_url,
                                "reason": "no_event_payload_found",
                                **_page_debug_snippet(result.html or "", getattr(result, "markdown", None)),
                            }
                        )
                except Exception as error:
                    logger.exception("Humanitix crawl exception url=%s error=%s", clean_url, error)
                    parse_failures.append(
                        {
                            "url": clean_url,
                            "reason": "crawl_exception",
                            "error": str(error),
                        }
                    )

        await asyncio.gather(*(crawl_one(url) for url in urls))

    deduped: Dict[str, Dict[str, Any]] = {}
    for event in events:
        key = event.get("id") or f"{event.get('url')}:{event.get('startDate')}"
        deduped[str(key)] = event

    return {
        "events": list(deduped.values())[:limit],
        "parse_failures": parse_failures,
    }


async def ingest_humanitix_events_to_eagle(
    *,
    organization_id: str,
    workspace_id: str,
    events: List[Dict[str, Any]],
    parse_failures: Optional[List[Dict[str, Any]]] = None,
    persist: bool = False,
) -> Dict[str, Any]:
    eagle_api_base_url = os.getenv("EAGLE_API_BASE_URL", DEFAULT_EAGLE_API_BASE_URL).rstrip("/")
    endpoint_url = f"{eagle_api_base_url}/scraper/events/humanitix-import"
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
            "failed_count": 0,
            "events": events,
            "results": [],
            "failures": [],
            "parse_failures": parse_failures or [],
        }

    async with httpx.AsyncClient(timeout=120) as client:
        for start in range(0, len(events), batch_size):
            batch = events[start : start + batch_size]
            payload: Dict[str, Any] = {
                "events": batch,
                "parseFailures": parse_failures or [],
            }
            if organization_id:
                payload["organizationId"] = organization_id
            if workspace_id:
                payload["workspaceId"] = workspace_id

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
                results.append(
                    {
                        **batch_meta,
                        "eagle_response": eagle_response,
                    }
                )
                failures.extend(eagle_response.get("failures") or [])
            except httpx.HTTPStatusError as error:
                failures.append(
                    {
                        **batch_meta,
                        "status_code": error.response.status_code,
                        "response": error.response.text,
                    }
                )
            except Exception as error:
                failures.append(
                    {
                        **batch_meta,
                        "error": str(error),
                    }
                )

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
