import asyncio
import json
import logging
import os
import re
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Literal, Optional, Tuple
from urllib.parse import urljoin, urlparse

import httpx
from bs4 import BeautifulSoup
from dotenv import load_dotenv

from generic_mapper import ingest_generic_events_to_eagle

logger = logging.getLogger(__name__)

load_dotenv(Path(__file__).resolve().parent / ".env")

TEN_TIMES_BASE_URL = "https://10times.com"
TEN_TIMES_DEFAULT_LIST_URL = "https://10times.com/newyork-us/conferences"
TEN_TIMES_AJAX_URL = "https://10times.com/ajax"
BRIGHTDATA_REQUEST_URL = "https://api.brightdata.com/request"
TenTimesSource = Literal["auto", "html", "brightdata"]

EVENT_PATH_RE = re.compile(r"^/e[a-z0-9][a-z0-9-]*", re.IGNORECASE)
WALL_MARKERS = (
    "quick check",
    "you're human",
    "log in to continue",
    "recaptcha",
    "cf-mitigated",
    "just a moment",
)


def _headers(referer: Optional[str] = None, cookie: Optional[str] = None) -> Dict[str, str]:
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36"
        ),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
    }
    if referer:
        headers["Referer"] = referer
    if cookie:
        headers["Cookie"] = cookie
    return headers


def _brightdata_api_key() -> Optional[str]:
    return os.getenv("BRIGHTDATA_API_KEY")


def _brightdata_zone() -> str:
    return os.getenv("BRIGHTDATA_ZONE") or "web_unlocker1"


def _is_wall_html(html: str) -> bool:
    lowered = (html or "").lower()
    return any(marker in lowered for marker in WALL_MARKERS)


def _clean_text(value: Any) -> Optional[str]:
    if value is None:
        return None
    text = re.sub(r"\s+", " ", str(value)).strip()
    return text or None


def _clean_url(url: str, base_url: str = TEN_TIMES_BASE_URL) -> str:
    parsed = urlparse(urljoin(base_url, url))
    return f"{parsed.scheme}://{parsed.netloc}{parsed.path}"


def _normalize_event_url(url: str) -> Optional[str]:
    parsed = urlparse(urljoin(TEN_TIMES_BASE_URL, url))
    if parsed.netloc and parsed.netloc.lower() not in {"10times.com", "www.10times.com"}:
        return None
    if not EVENT_PATH_RE.match(parsed.path):
        return None
    return f"https://10times.com{parsed.path}"


def _parse_date_piece(day: str, month: str, year: str) -> Optional[str]:
    for fmt in ("%d %b %Y", "%d %B %Y"):
        try:
            return datetime.strptime(f"{day} {month} {year}", fmt).date().isoformat()
        except ValueError:
            continue
    return None


def _parse_date_range(text: Optional[str]) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    display = _clean_text(text)
    if not display:
        return None, None, None

    compact = re.sub(
        r"\b(?:Mon|Tue|Wed|Thu|Fri|Sat|Sun)(?:day)?\b,?\s*",
        "",
        display,
        flags=re.IGNORECASE,
    )

    full_matches = re.findall(r"(\d{1,2})\s+([A-Za-z]{3,9})\s+(\d{4})", compact)
    if len(full_matches) >= 2:
        start = _parse_date_piece(*full_matches[0])
        end = _parse_date_piece(*full_matches[-1])
        return start, end, display
    if len(full_matches) == 1:
        end_day, end_month, end_year = full_matches[0]
        end = _parse_date_piece(end_day, end_month, end_year)
        range_match = re.search(r"(\d{1,2})\s*-\s*\d{1,2}\s+[A-Za-z]{3,9}\s+\d{4}", compact)
        if range_match:
            start = _parse_date_piece(range_match.group(1), end_month, end_year)
            return start, end, display
        return end, end, display

    return None, None, display


def _extract_json_ld_events(html: str, source_url: str) -> List[Dict[str, Any]]:
    soup = BeautifulSoup(html or "", "html.parser")
    events: List[Dict[str, Any]] = []
    for script in soup.find_all("script", attrs={"type": "application/ld+json"}):
        raw = script.string or script.get_text()
        if not raw:
            continue
        try:
            parsed = json.loads(raw)
        except Exception:
            continue
        queue = parsed if isinstance(parsed, list) else [parsed]
        for item in queue:
            if isinstance(item, dict) and item.get("@graph"):
                queue.extend(item["@graph"])
                continue
            types = item.get("@type") if isinstance(item, dict) else None
            type_values = types if isinstance(types, list) else [types]
            if any(str(value).lower() == "event" for value in type_values):
                event = dict(item)
                event.setdefault("url", source_url)
                event.setdefault("sourceUrl", source_url)
                events.append(event)
    return events


async def _fetch_direct(client: httpx.AsyncClient, url: str, *, referer: Optional[str], cookie: Optional[str]) -> Tuple[Optional[str], Optional[Dict[str, Any]]]:
    try:
        response = await client.get(url, headers=_headers(referer=referer, cookie=cookie), follow_redirects=True)
        text = response.text
        if response.status_code >= 400:
            return None, {"url": url, "reason": f"http_status:{response.status_code}", "body_preview": text[:300]}
        if _is_wall_html(text):
            return None, {"url": url, "reason": "human_check_or_captcha_wall", "body_preview": _clean_text(text[:500])}
        return text, None
    except Exception as error:
        return None, {"url": url, "reason": "direct_fetch_exception", "error": str(error)}


async def _fetch_brightdata(client: httpx.AsyncClient, url: str, *, referer: Optional[str], cookie: Optional[str]) -> Tuple[Optional[str], Optional[Dict[str, Any]]]:
    api_key = _brightdata_api_key()
    if not api_key:
        return None, {"url": url, "reason": "brightdata_api_key_missing"}

    payload: Dict[str, Any] = {
        "zone": _brightdata_zone(),
        "url": url,
        "format": "raw",
        "country": "us",
    }
    headers = _headers(referer=referer, cookie=cookie)
    if headers:
        payload["headers"] = headers

    try:
        response = await client.post(
            BRIGHTDATA_REQUEST_URL,
            json=payload,
            headers={"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"},
            timeout=120,
        )
        text = response.text
        if response.status_code >= 400:
            return None, {"url": url, "reason": f"brightdata_status:{response.status_code}", "body_preview": text[:500]}
        if _is_wall_html(text):
            return None, {"url": url, "reason": "brightdata_returned_human_check_or_captcha_wall", "body_preview": _clean_text(text[:500])}
        return text, None
    except Exception as error:
        return None, {"url": url, "reason": "brightdata_fetch_exception", "error": str(error)}


async def _fetch_html(
    client: httpx.AsyncClient,
    url: str,
    *,
    source: TenTimesSource,
    referer: Optional[str] = None,
    cookie: Optional[str] = None,
) -> Tuple[Optional[str], List[Dict[str, Any]]]:
    failures: List[Dict[str, Any]] = []
    if source in ("auto", "html"):
        html, failure = await _fetch_direct(client, url, referer=referer, cookie=cookie)
        if html:
            return html, failures
        if failure:
            failures.append({**failure, "source": "direct"})
        if source == "html":
            return None, failures

    html, failure = await _fetch_brightdata(client, url, referer=referer, cookie=cookie)
    if html:
        return html, failures
    if failure:
        failures.append({**failure, "source": "brightdata"})
    return None, failures


def _card_scope(anchor: Any) -> Any:
    current = anchor
    for _ in range(6):
        parent = getattr(current, "parent", None)
        if not parent:
            break
        text = parent.get_text(" ", strip=True)
        if len(text) > 80:
            return parent
        current = parent
    return anchor.parent or anchor


def _extract_categories(scope: Any) -> List[str]:
    categories: List[str] = []
    for node in scope.select(".badge, .label, .tag, small, a, span"):
        text = _clean_text(node.get_text(" ", strip=True))
        if not text:
            continue
        if len(text) <= 45 and not re.search(r"\d{4}|Miles|Members|Interested|Premium", text, re.I):
            categories.append(text)
    return list(dict.fromkeys(categories))[:8]


def _parse_list_events(html: str, list_url: str) -> Tuple[List[Dict[str, Any]], List[str]]:
    soup = BeautifulSoup(html or "", "html.parser")
    events: List[Dict[str, Any]] = []
    detail_urls: List[str] = []
    seen: set[str] = set()

    for anchor in soup.find_all("a", href=True):
        source_url = _normalize_event_url(anchor["href"])
        title = _clean_text(anchor.get_text(" ", strip=True))
        if not source_url or not title:
            continue
        if source_url in seen:
            continue
        seen.add(source_url)
        detail_urls.append(source_url)

        scope = _card_scope(anchor)
        text = _clean_text(scope.get_text(" ", strip=True)) or ""
        date_line = None
        for candidate in re.split(r"\s{2,}|\n", scope.get_text("\n", strip=True)):
            if re.search(r"\b\d{1,2}\s+[A-Za-z]{3,9}\s+\d{4}\b", candidate) or re.search(r"\b\d{1,2}\s*-\s*\w+", candidate):
                date_line = candidate
                break
        start_at, end_at, display_date = _parse_date_range(date_line or text[:90])

        location_text = None
        distance_text = None
        distance_match = re.search(r"(\d+(?:\.\d+)?)\s*Miles?\s+from\s+([^|]+)", text, re.I)
        if distance_match:
            distance_text = distance_match.group(0)
            location_text = distance_match.group(2).strip()
        else:
            parts = [part.strip() for part in scope.get_text("\n", strip=True).splitlines() if part.strip()]
            for part in parts:
                if part != title and not re.search(r"\d{4}|Interested|Premium|Share|Follow", part, re.I):
                    location_text = part
                    break

        description = text
        if title in description:
            description = description.split(title, 1)[-1].strip()
        if distance_text:
            description = description.replace(distance_text, "").strip()
        description = description[:1200] if description else None

        events.append(
            {
                "name": title,
                "url": source_url,
                "sourceUrl": source_url,
                "sourceProvider": "10times",
                "eventType": "Conference",
                "startDate": start_at,
                "endDate": end_at or start_at,
                "locationText": location_text,
                "city": location_text,
                "country": "US",
                "description": description,
                "categories": _extract_categories(scope),
                "displayStartAt": display_date,
                "metadata": {
                    "source": "10times",
                    "listUrl": list_url,
                    "cardText": text[:2000],
                    "distanceText": distance_text,
                },
            }
        )
    return events, detail_urls


def _merge_event(base: Dict[str, Any], detail: Dict[str, Any]) -> Dict[str, Any]:
    merged = {**base}
    for key, value in detail.items():
        if value not in (None, "", [], {}):
            merged[key] = value
    metadata = {**(base.get("metadata") or {}), **(detail.get("metadata") or {})}
    if metadata:
        merged["metadata"] = metadata
    return merged


def _parse_detail_event(html: str, source_url: str) -> Dict[str, Any]:
    json_ld_events = _extract_json_ld_events(html, source_url)
    if json_ld_events:
        event = json_ld_events[0]
        event["metadata"] = {"source": "10times", "jsonLd": event}
        return event

    soup = BeautifulSoup(html or "", "html.parser")
    title = _clean_text((soup.find("h1") or soup.find("title") or soup).get_text(" ", strip=True))
    text = _clean_text(soup.get_text(" ", strip=True)) or ""
    start_at, end_at, display_date = _parse_date_range(text[:300])
    description_node = soup.find(attrs={"name": "description"})
    description = description_node.get("content") if description_node else None

    return {
        "name": title,
        "url": source_url,
        "sourceUrl": source_url,
        "sourceProvider": "10times",
        "eventType": "Conference",
        "startDate": start_at,
        "endDate": end_at or start_at,
        "description": _clean_text(description) or text[:1200],
        "metadata": {
            "source": "10times",
            "displayStartAt": display_date,
            "detailText": text[:3000],
        },
    }


def _dedupe_events(events: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    deduped: List[Dict[str, Any]] = []
    seen: set[str] = set()
    for event in events:
        key = event.get("sourceUrl") or event.get("url") or event.get("name")
        if not key or key in seen:
            continue
        seen.add(str(key))
        deduped.append(event)
    return deduped


async def crawl_ten_times_events_with_diagnostics(
    *,
    list_url: str = TEN_TIMES_DEFAULT_LIST_URL,
    source: TenTimesSource = "auto",
    limit: int = 50,
    pages: int = 1,
    enrich_details: bool = True,
    cookie: Optional[str] = None,
) -> Dict[str, Any]:
    diagnostics: Dict[str, Any] = {
        "source": source,
        "list_url": list_url,
        "pages_requested": pages,
        "direct_first": source in ("auto", "html"),
        "brightdata_fallback": source in ("auto", "brightdata"),
        "list_pages_fetched": 0,
        "detail_pages_fetched": 0,
    }
    failures: List[Dict[str, Any]] = []
    parse_failures: List[Dict[str, Any]] = []
    events_by_url: Dict[str, Dict[str, Any]] = {}

    async with httpx.AsyncClient(timeout=45) as client:
        html, fetch_failures = await _fetch_html(client, list_url, source=source, cookie=cookie)
        failures.extend(fetch_failures)
        if not html:
            return {"events": [], "parse_failures": failures, "diagnostics": diagnostics}

        diagnostics["list_pages_fetched"] += 1
        list_events, detail_urls = _parse_list_events(html, list_url)
        for event in list_events:
            if event.get("sourceUrl"):
                events_by_url[event["sourceUrl"]] = event

        parsed_path = urlparse(list_url).path or "/newyork-us/conferences"
        for page in range(2, max(1, pages) + 1):
            if len(events_by_url) >= limit:
                break
            ajax_url = f"{TEN_TIMES_AJAX_URL}?for=scroll&path={parsed_path}&page={page}&ajax=1"
            ajax_html, ajax_failures = await _fetch_html(client, ajax_url, source=source, referer=list_url, cookie=cookie)
            failures.extend(ajax_failures)
            if not ajax_html:
                continue
            diagnostics["list_pages_fetched"] += 1
            page_events, page_detail_urls = _parse_list_events(ajax_html, list_url)
            detail_urls.extend(page_detail_urls)
            for event in page_events:
                if event.get("sourceUrl"):
                    events_by_url.setdefault(event["sourceUrl"], event)

        detail_urls = list(dict.fromkeys(detail_urls))[:limit]
        if enrich_details and detail_urls:
            semaphore = asyncio.Semaphore(3)

            async def fetch_detail(url: str) -> None:
                async with semaphore:
                    detail_html, detail_failures = await _fetch_html(client, url, source=source, referer=list_url, cookie=cookie)
                    failures.extend(detail_failures)
                    if not detail_html:
                        return
                    diagnostics["detail_pages_fetched"] += 1
                    detail_event = _parse_detail_event(detail_html, url)
                    if url in events_by_url:
                        events_by_url[url] = _merge_event(events_by_url[url], detail_event)
                    else:
                        events_by_url[url] = detail_event

            await asyncio.gather(*(fetch_detail(url) for url in detail_urls))

    events = _dedupe_events(list(events_by_url.values()))[:limit]
    if not events and not failures:
        parse_failures.append({"reason": "no_events_found", "list_url": list_url})

    return {
        "events": events,
        "parse_failures": [*parse_failures, *failures],
        "diagnostics": diagnostics,
    }


async def ingest_ten_times_events_to_eagle(
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
        source_provider="10times",
        parse_failures=parse_failures,
        persist=persist,
    )
    response["diagnostics"] = diagnostics or {}
    return response
