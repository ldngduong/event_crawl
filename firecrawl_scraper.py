import asyncio
import json
import logging
import math
import os
import re
from datetime import datetime
from html import unescape
from typing import Any, Dict, Iterable, List, Optional, Pattern, Tuple
from urllib.parse import urljoin, urlparse, urlunparse

import httpx
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)

DEFAULT_EAGLE_API_BASE_URL = "http://localhost:3001/api/v1"
DEFAULT_EAGLE_IMPORT_BATCH_SIZE = 20
DEFAULT_FIRECRAWL_API_URL = "https://api.firecrawl.dev/v2"


def _firecrawl_api_base_url() -> str:
    base_url = os.getenv("FIRECRAWL_API_URL", DEFAULT_FIRECRAWL_API_URL).rstrip("/")
    return base_url if base_url.endswith("/v2") else f"{base_url}/v2"


def _clean_url(url: str, base_url: Optional[str] = None) -> str:
    joined = urljoin(base_url or "", unescape(url or "").strip())
    parsed = urlparse(joined)
    return urlunparse((parsed.scheme, parsed.netloc, parsed.path.rstrip("/"), "", parsed.query, ""))


def _same_domain(url: str, list_url: str) -> bool:
    return urlparse(url).netloc.lower() == urlparse(list_url).netloc.lower()


def _first_string(*values: Any) -> Optional[str]:
    for value in values:
        if isinstance(value, str) and value.strip():
            return value.strip()
        if isinstance(value, (int, float)) and math.isfinite(float(value)):
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


def _strip_html(value: Any) -> Optional[str]:
    if not isinstance(value, str):
        return None
    text = BeautifulSoup(value, "html.parser").get_text(" ", strip=True)
    return text or None


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
    return "event" in str(value).lower()


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


def _extract_json_ld_events(html: str, source_url: str, fallback_event_type: str) -> List[Dict[str, Any]]:
    soup = BeautifulSoup(html or "", "html.parser")
    events: List[Dict[str, Any]] = []
    for script in soup.find_all("script", attrs={"type": "application/ld+json"}):
        payload = _json_loads_safe(script.string or script.get_text(" ", strip=True) or "")
        for event in _iter_json_ld_events(payload):
            events.append(_compact_json_ld_event(event, source_url, fallback_event_type))
    return events


def _compact_json_ld_event(event: Dict[str, Any], source_url: str, fallback_event_type: str) -> Dict[str, Any]:
    clean_source_url = _clean_url(_first_string(event.get("url"), source_url) or source_url, source_url)
    organizer = event.get("organizer") if isinstance(event.get("organizer"), dict) else {}
    location = _normalize_location(event.get("location"))
    image = event.get("image")
    categories = event.get("keywords")
    category_list: List[str] = []
    if isinstance(categories, str):
        category_list = [part.strip() for part in re.split(r"[,|]", categories) if part.strip()]
    elif isinstance(categories, list):
        category_list = [str(part).strip() for part in categories if str(part).strip()]

    return {
        "@type": "Event",
        "id": _first_string(event.get("@id"), event.get("identifier"), clean_source_url),
        "name": _first_string(event.get("name"), event.get("headline")),
        "title": _first_string(event.get("name"), event.get("headline")),
        "url": clean_source_url,
        "source_url": clean_source_url,
        "startDate": _first_string(event.get("startDate")),
        "endDate": _first_string(event.get("endDate")),
        "description": _strip_html(event.get("description")),
        "image": _first_string(*(image if isinstance(image, list) else [image])),
        "location": location,
        "organizer": {
            "@type": "Organization",
            "name": _first_string(organizer.get("name")),
            "url": _first_string(organizer.get("url")),
            "email": _first_string(organizer.get("email")),
            "telephone": _first_string(organizer.get("telephone")),
        },
        "categories": category_list,
        "eventType": category_list[0] if category_list else fallback_event_type,
        "country": _first_string(location.get("address", {}).get("addressCountry")),
        "firecrawl": {
            "extraction_source": "json_ld",
        },
    }


def _meta_content(soup: BeautifulSoup, *selectors: Tuple[str, str]) -> Optional[str]:
    for attr, value in selectors:
        tag = soup.find("meta", attrs={attr: value})
        content = tag.get("content") if tag else None
        if isinstance(content, str) and content.strip():
            return content.strip()
    return None


def _fallback_event_from_page(
    *,
    html: str,
    markdown: str,
    metadata: Dict[str, Any],
    source_url: str,
    fallback_event_type: str,
) -> Optional[Dict[str, Any]]:
    soup = BeautifulSoup(html or "", "html.parser")
    title = _first_string(
        metadata.get("title"),
        _meta_content(soup, ("property", "og:title"), ("name", "twitter:title")),
        soup.find("h1").get_text(" ", strip=True) if soup.find("h1") else None,
        soup.title.get_text(" ", strip=True) if soup.title else None,
    )
    if not title:
        return None

    title = re.sub(r"\s*\|\s*.+$", "", title).strip()
    description = _first_string(
        metadata.get("description"),
        _meta_content(soup, ("name", "description"), ("property", "og:description"), ("name", "twitter:description")),
    )

    return {
        "@type": "Event",
        "id": _clean_url(source_url),
        "name": title,
        "title": title,
        "url": _clean_url(source_url),
        "source_url": _clean_url(source_url),
        "description": description,
        "location": _guess_location_from_markdown(markdown),
        "eventType": fallback_event_type,
        "firecrawl": {
            "extraction_source": "meta_fallback",
        },
    }


def _guess_location_from_markdown(markdown: str) -> Dict[str, Any]:
    lines = [line.strip(" -*|") for line in (markdown or "").splitlines()]
    for index, line in enumerate(lines):
        if re.search(r"\b(venue|location|city|address)\b", line, re.IGNORECASE):
            for candidate in lines[index + 1 : index + 4]:
                if candidate and len(candidate) <= 180:
                    return {"name": candidate}
    return {}


def _extract_detail_links(
    *,
    list_url: str,
    html: str,
    links: List[str],
    markdown: str,
    limit: int,
    event_url_regex: Optional[str],
    include_url_patterns: List[str],
    exclude_url_patterns: List[str],
    same_domain_only: bool,
) -> List[str]:
    candidates: List[str] = []
    compiled: Optional[Pattern[str]] = re.compile(event_url_regex, re.IGNORECASE) if event_url_regex else None

    soup = BeautifulSoup(html or "", "html.parser")
    raw_urls = list(links or [])
    raw_urls.extend(str(link.get("href") or "") for link in soup.find_all("a", href=True))
    raw_urls.extend(match.group(0) for match in re.finditer(r"https?://[^\s\]\)\"'<>]+", markdown or ""))

    for raw_url in raw_urls:
        clean = _clean_url(raw_url, list_url)
        if not urlparse(clean).scheme.startswith("http"):
            continue
        if same_domain_only and not _same_domain(clean, list_url):
            continue
        if compiled and not compiled.search(clean):
            continue
        if include_url_patterns and not any(pattern in clean for pattern in include_url_patterns):
            continue
        if exclude_url_patterns and any(pattern in clean for pattern in exclude_url_patterns):
            continue
        if clean == _clean_url(list_url):
            continue
        if clean not in candidates:
            candidates.append(clean)
            if len(candidates) >= limit:
                break

    return candidates


async def _scrape_firecrawl(
    *,
    client: httpx.AsyncClient,
    url: str,
    wait_for_ms: int,
    timeout_ms: int,
    max_age_ms: int,
    proxy: Optional[str],
    location_country: Optional[str],
    location_languages: List[str],
) -> Dict[str, Any]:
    api_key = os.getenv("FIRECRAWL_API_KEY")
    if not api_key:
        raise RuntimeError("Missing FIRECRAWL_API_KEY")

    payload: Dict[str, Any] = {
        "url": url,
        "formats": ["markdown", "html", "links"],
        "onlyMainContent": True,
        "onlyCleanContent": False,
        "waitFor": wait_for_ms,
        "timeout": timeout_ms,
        "removeBase64Images": True,
        "blockAds": True,
        "storeInCache": True,
    }
    if max_age_ms > 0:
        payload["maxAge"] = max_age_ms
    if proxy:
        payload["proxy"] = proxy
    if location_country or location_languages:
        payload["location"] = {
            "country": location_country or "US",
            "languages": location_languages or ["en-US"],
        }

    response = await client.post(
        f"{_firecrawl_api_base_url()}/scrape",
        json=payload,
        headers={
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
        },
    )
    response.raise_for_status()
    body = response.json()
    if body.get("success") is False:
        raise RuntimeError(body.get("error") or body.get("message") or "Firecrawl scrape failed")
    return body.get("data") if isinstance(body.get("data"), dict) else body


def _normalize_event(
    *,
    event: Dict[str, Any],
    source_provider: str,
    list_url: str,
    detail_url: str,
    raw_metadata: Dict[str, Any],
) -> Dict[str, Any]:
    payload = dict(event)
    payload["url"] = _first_string(payload.get("url"), payload.get("source_url"), detail_url)
    payload["source_url"] = _first_string(payload.get("source_url"), payload.get("url"), detail_url)
    payload["sourceUrl"] = payload["source_url"]
    payload["source_provider"] = source_provider

    location = payload.get("location") if isinstance(payload.get("location"), dict) else {}
    address = location.get("address") if isinstance(location.get("address"), dict) else {}
    payload["city"] = _first_string(payload.get("city"), address.get("addressLocality"))
    payload["country"] = _first_string(payload.get("country"), address.get("addressCountry"))
    payload["metadata"] = {
        "sourceProvider": source_provider,
        "listUrl": list_url,
        "detailUrl": detail_url,
        "firecrawl": {
            **(payload.get("firecrawl") if isinstance(payload.get("firecrawl"), dict) else {}),
            "metadata": raw_metadata,
            "importedAt": datetime.utcnow().isoformat() + "Z",
        },
    }
    return {key: value for key, value in payload.items() if value not in (None, "", [], {})}


async def crawl_firecrawl_events_with_diagnostics(
    *,
    list_url: str,
    limit: int,
    event_url_regex: Optional[str] = None,
    include_url_patterns: Optional[List[str]] = None,
    exclude_url_patterns: Optional[List[str]] = None,
    same_domain_only: bool = True,
    enrich_details: bool = True,
    detail_concurrency: int = 2,
    wait_for_ms: int = 4000,
    timeout_ms: int = 60000,
    max_age_ms: int = 86_400_000,
    firecrawl_proxy: Optional[str] = "auto",
    location_country: Optional[str] = "US",
    location_languages: Optional[List[str]] = None,
    source_provider: str = "firecrawl",
    event_type: str = "Conference",
) -> Dict[str, Any]:
    parse_failures: List[Dict[str, Any]] = []
    diagnostics: Dict[str, Any] = {
        "list_url": list_url,
        "firecrawl_calls_attempted": 0,
        "estimated_credit_floor": 1,
    }

    async with httpx.AsyncClient(timeout=(timeout_ms / 1000) + 30) as client:
        diagnostics["firecrawl_calls_attempted"] += 1
        list_data = await _scrape_firecrawl(
            client=client,
            url=list_url,
            wait_for_ms=wait_for_ms,
            timeout_ms=timeout_ms,
            max_age_ms=max_age_ms,
            proxy=firecrawl_proxy,
            location_country=location_country,
            location_languages=location_languages or ["en-US"],
        )

        detail_urls = _extract_detail_links(
            list_url=list_url,
            html=str(list_data.get("html") or ""),
            links=[str(link) for link in (list_data.get("links") or [])],
            markdown=str(list_data.get("markdown") or ""),
            limit=limit,
            event_url_regex=event_url_regex,
            include_url_patterns=include_url_patterns or [],
            exclude_url_patterns=exclude_url_patterns or [],
            same_domain_only=same_domain_only,
        )

        diagnostics["detail_urls"] = detail_urls
        diagnostics["estimated_credit_floor"] = 1 + (len(detail_urls) if enrich_details else 0)

        if not enrich_details:
            return {
                "events": [],
                "parse_failures": parse_failures,
                "diagnostics": diagnostics,
            }

        semaphore = asyncio.Semaphore(detail_concurrency)

        async def scrape_detail(detail_url: str) -> Optional[Dict[str, Any]]:
            async with semaphore:
                try:
                    diagnostics["firecrawl_calls_attempted"] += 1
                    detail_data = await _scrape_firecrawl(
                        client=client,
                        url=detail_url,
                        wait_for_ms=wait_for_ms,
                        timeout_ms=timeout_ms,
                        max_age_ms=max_age_ms,
                        proxy=firecrawl_proxy,
                        location_country=location_country,
                        location_languages=location_languages or ["en-US"],
                    )
                    html = str(detail_data.get("html") or "")
                    markdown = str(detail_data.get("markdown") or "")
                    metadata = detail_data.get("metadata") if isinstance(detail_data.get("metadata"), dict) else {}
                    events = _extract_json_ld_events(html, detail_url, event_type)
                    event = events[0] if events else _fallback_event_from_page(
                        html=html,
                        markdown=markdown,
                        metadata=metadata,
                        source_url=detail_url,
                        fallback_event_type=event_type,
                    )
                    if not event:
                        parse_failures.append({"url": detail_url, "reason": "no_event_data_extracted"})
                        return None
                    return _normalize_event(
                        event=event,
                        source_provider=source_provider,
                        list_url=list_url,
                        detail_url=detail_url,
                        raw_metadata=metadata,
                    )
                except Exception as error:
                    parse_failures.append({"url": detail_url, "reason": str(error)})
                    return None

        events = [event for event in await asyncio.gather(*(scrape_detail(url) for url in detail_urls)) if event]
        deduped: Dict[str, Dict[str, Any]] = {}
        for event in events:
            key = _first_string(event.get("source_url"), event.get("url"), event.get("name")) or str(len(deduped))
            deduped[key] = event

        return {
            "events": list(deduped.values())[:limit],
            "parse_failures": parse_failures,
            "diagnostics": diagnostics,
        }


def _sanitize_event_for_eagle(event: Dict[str, Any]) -> Dict[str, Any]:
    payload = dict(event)
    description = payload.get("description")
    if isinstance(description, str) and len(description) > 5000:
        payload["description"] = description[:5000]
    return payload


async def ingest_firecrawl_events_to_eagle(
    *,
    organization_id: Optional[str],
    workspace_id: Optional[str],
    events: List[Dict[str, Any]],
    parse_failures: Optional[List[Dict[str, Any]]] = None,
    diagnostics: Optional[Dict[str, Any]] = None,
    persist: bool = False,
) -> Dict[str, Any]:
    eagle_api_base_url = os.getenv("EAGLE_API_BASE_URL", DEFAULT_EAGLE_API_BASE_URL).rstrip("/")
    endpoint_url = os.getenv("EAGLE_FIRECRAWL_IMPORT_URL") or f"{eagle_api_base_url}/scraper/events/discover-events-import"
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
            batch = [_sanitize_event_for_eagle(event) for event in events[start : start + batch_size]]
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
                results.append({**batch_meta, "eagle_response": eagle_response})
                eagle_data = eagle_response.get("data") if isinstance(eagle_response.get("data"), dict) else eagle_response
                failures.extend(eagle_data.get("failures") or [])
            except httpx.HTTPStatusError as error:
                failures.append({**batch_meta, "status_code": error.response.status_code, "response": error.response.text})
            except Exception as error:
                failures.append({**batch_meta, "error": str(error)})

    imported_count = 0
    created_count = 0
    updated_count = 0
    skipped_count = 0
    for result in results:
        eagle_response = result.get("eagle_response", {})
        eagle_data = eagle_response.get("data") if isinstance(eagle_response.get("data"), dict) else eagle_response
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
        "diagnostics": diagnostics or {},
    }
