import asyncio
import json
import logging
import os
import re
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Literal, Optional, Tuple
from urllib.parse import urlencode, urljoin, urlparse

import httpx
from bs4 import BeautifulSoup
from dotenv import load_dotenv

from generic_mapper import ingest_generic_events_to_eagle
from schemas import GenericAttendeeDict, GenericCompanyDict, GenericMappedEventDict, GenericOccurrenceDict

logger = logging.getLogger(__name__)

load_dotenv(Path(__file__).resolve().parent / ".env")

TEN_TIMES_BASE_URL = "https://10times.com"
TEN_TIMES_DEFAULT_LIST_URL = "https://10times.com/newyork-us/conferences"
TEN_TIMES_AJAX_URL = "https://10times.com/ajax"
BRIGHTDATA_REQUEST_URL = "https://api.brightdata.com/request"
TenTimesSource = Literal["auto", "html", "brightdata"]

EVENT_PATH_RE = re.compile(r"^/(?:e\d[a-z0-9-]*|[a-z0-9][a-z0-9-]{2,})(?:/)?$", re.IGNORECASE)
LISTING_PATH_RE = re.compile(
    r"/(?:conferences|tradeshows|trade-shows|workshops|seminars|events|exhibitions|webinars|summits|meetups)(?:/)?$",
    re.IGNORECASE,
)
LOCATION_PATH_RE = re.compile(r"^/[a-z0-9][a-z0-9-]*-[a-z]{2}(?:/)?$", re.IGNORECASE)
WALL_MARKERS = (
    "quick check",
    "you're human",
    "too many requests",
    "unusual traffic",
    "access denied",
    "account suspended",
    "cf-mitigated",
    "just a moment",
)
LIST_NOISE_MARKERS = (
    "featured events",
    "premium",
    "for partners",
    "advertise",
    "who's in town",
    "explore",
    "recommended",
    "similar events",
    "nearby",
)
EVENT_ROW_SELECTORS = (
    "article",
    "li",
    "tr",
    ".event-card",
    ".event-list-item",
    ".event-item",
    ".event",
    ".card",
    ".row",
    "[class*='event']",
    "[id*='event']",
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


def _ajax_headers(referer: str, cookie: Optional[str] = None) -> Dict[str, str]:
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/147.0.0.0 Safari/537.36"
        ),
        "Accept": "*/*",
        "Accept-Encoding": "gzip, deflate, br, zstd",
        "Accept-Language": "en-US,en;q=0.5",
        "Access-Control-Allow-Headers": "Origin,Content-Type,Accept",
        "Priority": "u=1, i",
        "Referer": referer,
        "Sec-CH-UA": '"Brave";v="147", "Not.A/Brand";v="8", "Chromium";v="147"',
        "Sec-CH-UA-Arch": '"x86"',
        "Sec-CH-UA-Bitness": '"64"',
        "Sec-CH-UA-Full-Version-List": '"Brave";v="147.0.0.0", "Not.A/Brand";v="8.0.0.0", "Chromium";v="147.0.0.0"',
        "Sec-CH-UA-Mobile": "?0",
        "Sec-CH-UA-Model": '""',
        "Sec-CH-UA-Platform": '"Linux"',
        "Sec-CH-UA-Platform-Version": '""',
        "Sec-GPC": "1",
        "X-Requested-With": "XMLHttpRequest",
        "Sec-Fetch-Dest": "empty",
        "Sec-Fetch-Mode": "cors",
        "Sec-Fetch-Site": "same-origin",
    }
    headers["Cookie"] = cookie or (
        'user_profile_url=http://10times.com/profile/duong-le-71953853; _ctuid=5c87ffa6-f4cb-471e-afac-2acef406eb8d; page_visit=1; '
        'g_state={"i_l":0,"i_ll":1784687752922,"i_b":"+VKeHR8HF90khBhlRBCa5bofRuYn7nWDrCwCY8U3QhQ","i_e":{"enable_itp_optimization":24},"i_et":1784687752922,"i_t":1784774119809}; '
        'image_flag=aHR0cHM6Ly9jMS4xMHRpbWVzLmNvbS9pbWcvbm8tcGljLmpwZw%3D%3D; user_token=8xRQfAW0RBj7LPE2sK3DD86ifIIUsIGRzG%2BRVz4JPqw%3D; '
        'user=71954891; user_flag=2; tenT_ip=14.232.161.96; email=g%2A%2A%2A%2A%2A%2A%2A%2A%2A%2A%2A%2A%2A%2A%2A%2Ah%40gonrr.net; '
        'global_user_membership=null; 10T_ping=1#$No#$No#$Returning User Secure; 10T_verify=1; '
        'bannerEventIds_Z3JlZW4=%7B%22upcoming%22%3A%5B3270027%2C3265439%2C3156571%2C2619817%2C3215831%5D%2C%22past%22%3A%5B%5D%7D; '
        'bannerEventIds_Z3JlZW5wb3J0=%7B%22upcoming%22%3A%5B3101717%5D%2C%22past%22%3A%5B101914%2C128221%2C209909%2C307142%5D%7D; '
        'bannerEventIds_bmV3JTIweW9yaw=%7B%22upcoming%22%3A%5B206232%2C853106%2C3263190%2C2852673%2C1239710%5D%2C%22past%22%3A%5B%5D%7D; '
        'show_login_modal=24; to10x_pv=31; defaultPopupList=[]; _ctpuid=3b9efadb-4355-41f2-a78a-d3a2c36dfb56; '
        'cf_clearance=zEOHT1BabUtKh_Z4LJVX5oHHNNWMsAlpHEkjk2e_Zog-1784777256-1.2.1.1-_s_yqjqmJD6UFYh.iDEXSCvQ.lhz69eTT5V1Te99XR7ZPLzmtrSkTuKLjCrqdJCt6IUWotxFkCA9DN4XuN8zix7by69OA.jVsrcUvQjVf6l4VqyiA2wf8tQGUYpg5D2zmr7T8bXKL_ht8I4LsopeKfYvSmwv3wohekAeqHjCha6R9p0kcPMYGPeCw8pYrr9h8IxqaimZJs.XmqrHqhAjfjgJ0h.3ULATmfDTbeu.2z7DU8QkWUgQOaIwR_dIescsIuSkoPNujmGkq2yK_1MAJdDzcfqTviF2XwdYaWd1wZVaGqyA.gqSykbIlUWgPH5FtCfTA0ThHPERs_xZgGxqdDXz1VZZ8SExOvwuX7g.HqvNNMxyi29Mbj2YsLRaOrW1LQApf6_KM0nGyjoZu4B5_K4aQcryDWhKNMXF7urF53RI3KU_MMtkxzW_kudvg1poEfRvq5eFSD9UtiIdW4DZiA'
    )
    return headers


def _brightdata_api_key() -> Optional[str]:
    return os.getenv("BRIGHTDATA_API_KEY")


def _brightdata_zone() -> str:
    return os.getenv("BRIGHTDATA_ZONE") or "web_unlocker1"


def _brightdata_retries() -> int:
    try:
        return max(1, min(8, int(os.getenv("BRIGHTDATA_RETRIES") or "4")))
    except ValueError:
        return 4


def _looks_like_tentimes_content(html: str, soup: BeautifulSoup) -> bool:
    if soup.select_one("tr.event-card, .event-card"):
        return True
    page_text = _clean_text(soup.get_text(" ", strip=True)) or ""
    if re.search(r"\bConferences in\b", page_text, re.I) and re.search(r"\bEvents\b", page_text, re.I):
        return True
    meta_description = soup.find("meta", attrs={"name": "description"})
    description = (meta_description.get("content") or "") if meta_description else ""
    return bool(re.search(r"\bfind and compare\b.*\bconferences\b", description, re.I))


def _wall_reason(html: str) -> Optional[str]:
    soup = BeautifulSoup(html or "", "html.parser")
    if _looks_like_tentimes_content(html or "", soup):
        return None
    title_node = soup.find("title")
    title = _clean_text(title_node.get_text(" ", strip=True)) if title_node else ""
    body_text = _clean_text((soup.body or soup).get_text(" ", strip=True)) or ""
    challenge_text = f"{title} {body_text[:2000]}".lower()
    if "cf-mitigated" in (html or "").lower():
        return "cf_mitigated"
    for marker in WALL_MARKERS:
        if marker in challenge_text:
            return marker.replace(" ", "_")
    if re.search(r"\b(?:verify|checking)\s+(?:you are|that you are|your browser)\s+human\b", challenge_text, re.I):
        return "human_verification"
    return None


def _is_wall_html(html: str) -> bool:
    return _wall_reason(html) is not None


def _clean_text(value: Any) -> Optional[str]:
    if value is None:
        return None
    text = re.sub(r"\s+", " ", str(value)).strip()
    return text or None


def _text_lines(scope: Any) -> List[str]:
    return [
        _clean_text(line) or ""
        for line in scope.get_text("\n", strip=True).splitlines()
        if _clean_text(line)
    ]


def _has_date_text(text: str) -> bool:
    return bool(
        re.search(r"\b\d{1,2}\s+[A-Za-z]{3,9}\s+\d{4}\b", text)
        or re.search(r"\b\d{1,2}\s*[-–]\s*(?:\d{1,2}\s+)?[A-Za-z]{3,9}\s+\d{4}\b", text)
        or re.search(r"\b(?:Mon|Tue|Wed|Thu|Fri|Sat|Sun),?\s+\d{1,2}\s+[A-Za-z]{3,9}\s+\d{4}\b", text, re.I)
    )


def _looks_like_event_title(title: Optional[str]) -> bool:
    if not title:
        return False
    if len(title) < 6 or len(title) > 180:
        return False
    if re.search(r"^events\s+in\s+", title, re.I):
        return False
    if re.search(r"^(share|follow|interested|bookmark|view|more|explore|premium|members?)$", title, re.I):
        return False
    return bool(re.search(r"[A-Za-z]", title))


def _clean_url(url: str, base_url: str = TEN_TIMES_BASE_URL) -> str:
    parsed = urlparse(urljoin(base_url, url))
    return f"{parsed.scheme}://{parsed.netloc}{parsed.path}"


def _normalize_event_url(url: str) -> Optional[str]:
    parsed = urlparse(urljoin(TEN_TIMES_BASE_URL, url))
    if parsed.netloc and parsed.netloc.lower() not in {"10times.com", "www.10times.com"}:
        return None
    if LISTING_PATH_RE.search(parsed.path) or LOCATION_PATH_RE.match(parsed.path):
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


def _parse_distance_miles(text: str) -> Optional[float]:
    match = re.search(r"(\d+(?:\.\d+)?)\s*Miles?\s+from", text, re.I)
    if not match:
        return None
    try:
        return float(match.group(1))
    except ValueError:
        return None


def _parse_attendance(text: str) -> Optional[int]:
    patterns = (
        r"(\d[\d,]*)\s+(?:Visitors|Attendees|Delegates|Members|Participants)",
        r"Estimated\s+(?:Turnout|Attendance)\s*:?\s*(\d[\d,]*)",
    )
    for pattern in patterns:
        match = re.search(pattern, text, re.I)
        if match:
            try:
                return int(match.group(1).replace(",", ""))
            except ValueError:
                return None
    return None


def _parse_city_country(location_text: Optional[str]) -> Tuple[Optional[str], Optional[str]]:
    if not location_text:
        return None, None
    text = _clean_text(location_text)
    if not text:
        return None, None
    parts = [part.strip() for part in re.split(r",|\|", text) if part.strip()]
    if len(parts) >= 2:
        return parts[0], parts[-1]
    if re.search(r"\bUSA?\b|United States", text, re.I):
        return text, "US"
    return text, None


def _looks_like_location_line(line: Optional[str]) -> bool:
    if not line:
        return False
    text = _clean_text(line) or ""
    if not text or len(text) > 80:
        return False
    if re.search(r"^events\s+in\s+", text, re.I):
        return False
    if _has_date_text(text):
        return False
    if re.search(r"Interested|Share|Follow|Premium|Members|Visitors|Speakers|Exhibitors|Miles from", text, re.I):
        return False
    if re.search(r"\b(?:Conference|Symposium|Summit|Festival|Expo|Workshop|Congress|Colloquium|Meeting)\b", text, re.I):
        return False
    return bool(re.search(r"[A-Za-z]", text))


def _extract_date_line(scope: Any) -> Optional[str]:
    for line in _text_lines(scope):
        if _has_date_text(line):
            return line
    text = _clean_text(scope.get_text(" ", strip=True)) or ""
    match = re.search(
        r"(?:Mon|Tue|Wed|Thu|Fri|Sat|Sun)?,?\s*\d{1,2}(?:\s*[-–]\s*\d{1,2})?\s+[A-Za-z]{3,9}\s+\d{4}",
        text,
        re.I,
    )
    return _clean_text(match.group(0)) if match else None


def _extract_location_from_scope(scope: Any, title: str, date_line: Optional[str]) -> Optional[str]:
    lines = _text_lines(scope)
    for index, line in enumerate(lines):
        if line == title:
            for candidate in lines[index + 1 : index + 4]:
                if candidate == date_line:
                    continue
                if _looks_like_location_line(candidate):
                    return candidate

    for line in lines:
        if line == title or line == date_line:
            continue
        if re.search(r"Interested|Share|Follow|Premium|Members|Visitors|Speakers|Exhibitors", line, re.I):
            continue
        if _has_date_text(line):
            continue
        if _looks_like_location_line(line) and ("," in line or re.search(r"\b(?:USA?|United States|York|City|Center|Hotel|Hall|Newark|Jersey)\b", line, re.I)):
            return line

    text = _clean_text(scope.get_text(" ", strip=True)) or ""
    distance_match = re.search(
        r"\d+(?:\.\d+)?\s*Miles?\s+from\s+([A-Za-z][A-Za-z .'-]{1,60}?)(?=\s+(?:The|This|A|An|Featuring|It|Education|Business|Power|Interested|$))",
        text,
        re.I,
    )
    if distance_match:
        return _clean_text(distance_match.group(1))
    return None


def _distance_text(text: str) -> Optional[str]:
    match = re.search(
        r"\d+(?:\.\d+)?\s*Miles?\s+from\s+[A-Za-z][A-Za-z .'-]{1,60}?(?=\s+(?:The|This|A|An|Featuring|It|Education|Business|Power|Interested|$))",
        text,
        re.I,
    )
    return _clean_text(match.group(0)) if match else None


def _extract_description(scope: Any, title: str, date_line: Optional[str], location_text: Optional[str]) -> Optional[str]:
    lines: List[str] = []
    for line in _text_lines(scope):
        if line in {title, date_line, location_text}:
            continue
        if re.search(r"^(Interested|Share|Follow|Premium|Members?|Visitors?|Speakers?|Exhibitors?)\b", line, re.I):
            continue
        if _has_date_text(line):
            continue
        if len(line) >= 40:
            lines.append(line)
    if lines:
        return _clean_text(" ".join(lines))[:1200]
    text = _clean_text(scope.get_text(" ", strip=True)) or ""
    for value in (title, date_line, location_text):
        if value:
            text = text.replace(value, " ")
    return (_clean_text(text) or "")[:1200] or None


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


def _first_string(*values: Any) -> Optional[str]:
    for value in values:
        if isinstance(value, list):
            nested = _first_string(*value)
            if nested:
                return nested
            continue
        if isinstance(value, dict):
            continue
        text = _clean_text(value)
        if text:
            return text
    return None


def _image_url(value: Any) -> Optional[str]:
    if isinstance(value, str):
        return _clean_url(value)
    if isinstance(value, list):
        for item in value:
            url = _image_url(item)
            if url:
                return url
    if isinstance(value, dict):
        return _first_string(value.get("url"), value.get("contentUrl"))
    return None


def _json_ld_to_event(raw_event: Dict[str, Any], source_url: str) -> Dict[str, Any]:
    location = raw_event.get("location") if isinstance(raw_event.get("location"), dict) else {}
    address = location.get("address") if isinstance(location.get("address"), dict) else {}
    organizer = raw_event.get("organizer") if isinstance(raw_event.get("organizer"), dict) else {}
    city = _first_string(address.get("addressLocality"))
    country = _first_string(address.get("addressCountry"))
    occurrence = {
        "locationText": _first_string(location.get("name"), address.get("streetAddress")),
        "venueName": _first_string(location.get("name")),
        "streetAddress": _first_string(address.get("streetAddress")),
        "city": city,
        "region": _first_string(address.get("addressRegion")),
        "postalCode": _first_string(address.get("postalCode")),
        "country": country,
        "latitude": location.get("latitude") or raw_event.get("latitude"),
        "longitude": location.get("longitude") or raw_event.get("longitude"),
        "expectedAttendance": _parse_attendance(json.dumps(raw_event, default=str)),
    }
    occurrence = {key: value for key, value in occurrence.items() if value not in (None, "", [], {})}
    return {
        "name": _first_string(raw_event.get("name"), raw_event.get("headline")) or "Unknown 10times Event",
        "url": source_url,
        "sourceUrl": source_url,
        "sourceProvider": "10times",
        "eventType": _first_string(raw_event.get("eventType"), raw_event.get("@type")) or "Conference",
        "startDate": _first_string(raw_event.get("startDate")),
        "endDate": _first_string(raw_event.get("endDate"), raw_event.get("startDate")),
        "city": city,
        "country": country,
        "description": _first_string(raw_event.get("description")),
        "organizerName": _first_string(organizer.get("name"), raw_event.get("organizerName")),
        "organizerWebsite": _first_string(organizer.get("url")),
        "eventImageUrl": _image_url(raw_event.get("image")),
        "expectedAttendance": occurrence.get("expectedAttendance"),
        "occurrence": occurrence,
        "metadata": {
            "source": "10times",
            "jsonLd": raw_event,
        },
    }


def _read_meta(soup: BeautifulSoup, *names: str) -> Optional[str]:
    for name in names:
        node = soup.find("meta", attrs={"property": name}) or soup.find("meta", attrs={"name": name})
        if node and node.get("content"):
            return _clean_text(node.get("content"))
    return None


def _main_detail_region(soup: BeautifulSoup) -> Any:
    h1 = soup.find("h1")
    if h1:
        current = h1
        best = h1.parent or h1
        for _ in range(8):
            parent = getattr(current, "parent", None)
            if not parent:
                break
            text = _clean_text(parent.get_text(" ", strip=True)) or ""
            if len(text) > 8000:
                break
            if not _is_noisy_scope(parent):
                best = parent
            current = parent
        return best
    return soup.select_one("main") or soup.select_one(".content") or soup


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
    }
    if cookie or referer:
        headers = _headers(referer=referer, cookie=cookie)
        payload["headers"] = headers

    last_failure: Optional[Dict[str, Any]] = None
    for attempt in range(1, _brightdata_retries() + 1):
        try:
            response = await client.post(
                BRIGHTDATA_REQUEST_URL,
                content=json.dumps(payload, separators=(",", ":")),
                headers={"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"},
                timeout=120,
            )
            text = response.text
            if response.status_code >= 400:
                return None, {"url": url, "reason": f"brightdata_status:{response.status_code}", "body_preview": text[:500]}
            if text.startswith("Request Failed (bad_endpoint)"):
                return None, {"url": url, "reason": "brightdata_bad_endpoint", "body_preview": _clean_text(text[:500])}

            wall_reason = _wall_reason(text)
            if wall_reason:
                last_failure = {
                    "url": url,
                    "reason": "brightdata_returned_human_check_or_captcha_wall",
                    "wall_reason": wall_reason,
                    "attempt": attempt,
                    "body_preview": _clean_text(text[:500]),
                }
                continue

            return text, None
        except Exception as error:
            last_failure = {"url": url, "reason": "brightdata_fetch_exception", "attempt": attempt, "error": str(error)}

    return None, last_failure or {"url": url, "reason": "brightdata_fetch_failed"}


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


async def _fetch_listing_page(
    client: httpx.AsyncClient,
    url: str,
    *,
    source: TenTimesSource,
    referer: str,
    cookie: Optional[str],
) -> Tuple[Optional[str], List[Dict[str, Any]]]:
    try:
        response = await client.get(url, headers=_ajax_headers(referer, cookie), follow_redirects=True)
        text = response.text
        if response.status_code >= 400:
            return None, [{"url": url, "reason": f"listing_ajax_status:{response.status_code}", "body_preview": text[:500], "source": "direct_cookie_ajax"}]
        if _is_wall_html(text):
            return None, [{"url": url, "reason": "listing_ajax_human_check_or_captcha_wall", "body_preview": _clean_text(text[:500]), "source": "direct_cookie_ajax"}]
        return text, []
    except Exception as error:
        return None, [{"url": url, "reason": "listing_ajax_fetch_exception", "error": str(error), "source": "direct_cookie_ajax"}]


def _is_noisy_scope(scope: Any) -> bool:
    text = (_clean_text(scope.get_text(" ", strip=True)) or "").lower()
    class_id = " ".join(
        str(value)
        for value in [
            scope.get("id"),
            " ".join(scope.get("class") or []),
            scope.parent.get("id") if getattr(scope, "parent", None) else None,
            " ".join(scope.parent.get("class") or []) if getattr(scope, "parent", None) else None,
        ]
        if value
    ).lower()
    if any(marker in class_id for marker in ("sidebar", "right", "premium", "featured", "sponsor", "ad-")):
        return True
    if any(marker in text[:220] for marker in LIST_NOISE_MARKERS):
        return True
    return False


def _event_card_scope(anchor: Any) -> Optional[Any]:
    title = _clean_text(anchor.get_text(" ", strip=True))
    if not _looks_like_event_title(title):
        return None

    best = None
    current = anchor
    for _ in range(8):
        parent = getattr(current, "parent", None)
        if not parent:
            break
        text = _clean_text(parent.get_text(" ", strip=True)) or ""
        if len(text) > 3500:
            break
        if _is_noisy_scope(parent):
            return None
        if _has_date_text(text) and title in text:
            best = parent
            if parent.name in {"article", "li", "tr"}:
                break
            classes = " ".join(parent.get("class") or [])
            if re.search(r"\b(event|card|row|item|listing)\b", classes, re.I):
                break
        current = parent

    if best is not None:
        return best

    for selector in EVENT_ROW_SELECTORS:
        scope = anchor.find_parent(selector)
        if not scope:
            continue
        text = _clean_text(scope.get_text(" ", strip=True)) or ""
        if title in text and _has_date_text(text) and not _is_noisy_scope(scope):
            return scope
    return None


def _extract_categories(scope: Any) -> List[str]:
    categories: List[str] = []
    nodes = scope.select("span.bg-light a, span.bg-light, .label, .tag")
    if not nodes:
        nodes = scope.select(".badge, .label, .tag, small")
    for node in nodes:
        text = _clean_text(node.get_text(" ", strip=True))
        if not text:
            continue
        if len(text) <= 45 and not re.search(r"\d{4}|Miles|Members|Interested|Premium|Share|Follow|^\d+(?:\.\d+)?$|^USA?$", text, re.I):
            categories.append(text)
    return list(dict.fromkeys(categories))[:8]


def _extract_event_url_from_card(scope: Any) -> Optional[str]:
    for node in scope.select("[onclick], [oncontextmenu], [data-url]"):
        for attr in ("data-url", "onclick", "oncontextmenu"):
            raw = node.get(attr)
            if not raw:
                continue
            match = re.search(r"https?://(?:www\.)?10times\.com/[a-z0-9][a-z0-9-]*", raw, re.I)
            if match:
                normalized = _normalize_event_url(match.group(0))
                if normalized:
                    return normalized
    for anchor in scope.find_all("a", href=True):
        normalized = _normalize_event_url(anchor.get("href", ""))
        if normalized:
            return normalized
    return None


def _extract_title_from_card(scope: Any) -> Optional[str]:
    for selector in ("h2 .d-block", "h2", "[data-ga-label] .d-block", "[data-name]"):
        node = scope.select_one(selector)
        if not node:
            continue
        text = node.get("data-name") if node.has_attr("data-name") else node.get_text(" ", strip=True)
        title = _clean_text(text)
        if _looks_like_event_title(title):
            return title
    return None


def _extract_venue_from_card(scope: Any) -> Optional[str]:
    venue = scope.select_one(".venue")
    if venue:
        text = _clean_text(venue.get_text(" ", strip=True))
        return re.sub(r"\s+,", ",", text) if text else None
    return None


def _extract_description_from_card(scope: Any) -> Optional[str]:
    for node in scope.select(".text-wrap.text-break, td.col-12.mt-3 div"):
        text = _clean_text(node.get_text(" ", strip=True))
        if text and len(text) >= 40 and not re.search(r"Interested|Share|Follow", text, re.I):
            return text[:1200]
    return None


def _parse_event_card(scope: Any, list_url: str) -> Optional[Dict[str, Any]]:
    source_url = _extract_event_url_from_card(scope)
    title = _extract_title_from_card(scope)
    if not source_url or not title:
        return None

    text = _clean_text(scope.get_text(" ", strip=True)) or ""
    date_node = scope.select_one("[data-start-date]")
    date_line = _clean_text(date_node.get_text(" ", strip=True)) if date_node else _extract_date_line(scope)
    start_at = date_node.get("data-start-date").replace("/", "-") if date_node and date_node.get("data-start-date") else None
    end_at = date_node.get("data-end-date").replace("/", "-") if date_node and date_node.get("data-end-date") else None
    if not start_at:
        start_at, end_at, _display = _parse_date_range(date_line or text[:140])
    else:
        _display = date_line

    location_text = _extract_venue_from_card(scope) or _extract_location_from_scope(scope, title, date_line)
    city, country = _parse_city_country(location_text)
    description = _extract_description_from_card(scope) or _extract_description(scope, title, date_line, location_text)
    expected_attendance = _parse_attendance(text)
    distance_text = _distance_text(text)

    return {
        "name": title,
        "url": source_url,
        "sourceUrl": source_url,
        "sourceProvider": "10times",
        "eventType": "Conference",
        "startDate": start_at,
        "endDate": end_at or start_at,
        "locationText": location_text,
        "city": city,
        "country": country or "US",
        "description": description,
        "expectedAttendance": expected_attendance,
        "categories": _extract_categories(scope),
        "displayStartAt": _display,
        "metadata": {
            "source": "10times",
            "listUrl": list_url,
            "cardText": text[:2000],
            "distanceMiles": _parse_distance_miles(text),
            "distanceText": distance_text,
            "eventId": re.search(r"event_(\d+)", " ".join(scope.get("class") or "")).group(1)
            if re.search(r"event_(\d+)", " ".join(scope.get("class") or ""))
            else None,
        },
    }


def _extract_main_event_region(soup: BeautifulSoup) -> Any:
    candidates = []
    for selector in (
        "main",
        "#events",
        "#event-list",
        ".event-list",
        ".events-list",
        ".listing",
        ".content",
        ".col-md-8",
        ".col-lg-8",
        ".col-sm-8",
        "body",
    ):
        for node in soup.select(selector):
            links = [
                link
                for link in node.find_all("a", href=True)
                if _normalize_event_url(link.get("href", ""))
                and _looks_like_event_title(_clean_text(link.get_text(" ", strip=True)))
            ]
            text = _clean_text(node.get_text(" ", strip=True)) or ""
            if links and _has_date_text(text):
                candidates.append((len(links), len(text), node))
    if not candidates:
        return soup
    candidates.sort(key=lambda item: (item[0], -item[1]), reverse=True)
    return candidates[0][2]


def _parse_list_events(html: str, list_url: str) -> Tuple[List[Dict[str, Any]], List[str]]:
    soup = BeautifulSoup(html or "", "html.parser")
    events: List[Dict[str, Any]] = []
    detail_urls: List[str] = []
    seen: set[str] = set()
    region = _extract_main_event_region(soup)

    for scope in region.select("tr.event-card, .event-card"):
        event = _parse_event_card(scope, list_url)
        if not event:
            continue
        source_url = event.get("sourceUrl")
        if not source_url or source_url in seen:
            continue
        seen.add(source_url)
        detail_urls.append(source_url)
        events.append(event)

    if events:
        return events, detail_urls

    for anchor in region.find_all("a", href=True):
        source_url = _normalize_event_url(anchor["href"])
        title = _clean_text(anchor.get_text(" ", strip=True))
        if not source_url or not _looks_like_event_title(title):
            continue
        if source_url in seen:
            continue

        scope = _event_card_scope(anchor)
        if scope is None:
            continue

        text = _clean_text(scope.get_text(" ", strip=True)) or ""
        if _is_noisy_scope(scope):
            continue
        seen.add(source_url)
        detail_urls.append(source_url)

        date_line = _extract_date_line(scope)
        start_at, end_at, display_date = _parse_date_range(date_line or text[:140])
        location_text = _extract_location_from_scope(scope, title or "", date_line)
        city, country = _parse_city_country(location_text)
        distance_miles = _parse_distance_miles(text)
        expected_attendance = _parse_attendance(text)
        description = _extract_description(scope, title or "", date_line, location_text)
        distance_text = _distance_text(text)

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
                "city": city,
                "country": country or "US",
                "description": description,
                "expectedAttendance": expected_attendance,
                "categories": _extract_categories(scope),
                "displayStartAt": display_date,
                "metadata": {
                    "source": "10times",
                    "listUrl": list_url,
                    "cardText": text[:2000],
                    "distanceMiles": distance_miles,
                    "distanceText": distance_text,
                },
            }
        )
    return events, detail_urls


def _extract_listing_pagination_ids(html: str) -> Tuple[List[str], List[str]]:
    soup = BeautifulSoup(html or "", "html.parser")
    event_ids: List[str] = []
    org_ids: List[str] = []

    for node in soup.select("[data-id][data-org], tr.event-card, .event-card"):
        data_id = node.get("data-id")
        data_org = node.get("data-org")
        if data_id and str(data_id).isdigit():
            event_ids.append(str(data_id))
        if data_org and str(data_org).isdigit():
            org_ids.append(str(data_org))

        for child in node.select("[data-id][data-org]"):
            child_id = child.get("data-id")
            child_org = child.get("data-org")
            if child_id and str(child_id).isdigit():
                event_ids.append(str(child_id))
            if child_org and str(child_org).isdigit():
                org_ids.append(str(child_org))

        classes = " ".join(node.get("class") or [])
        for event_id in re.findall(r"event_(\d+)", classes):
            event_ids.append(event_id)

    return list(dict.fromkeys(org_ids))[:60], list(dict.fromkeys(event_ids))[:60]


def _build_listing_ajax_url(list_url: str, page: int, html: str) -> Optional[str]:
    org_ids, event_ids = _extract_listing_pagination_ids(html)
    if not event_ids:
        return None

    parsed = urlparse(list_url)
    query = {
        "ajax": "2",
        "page": str(page),
        "f_e": ",".join(event_ids),
        "listing_pagination": "1",
    }
    if org_ids:
        query["f_c"] = ",".join(org_ids)
    return f"https://10times.com{parsed.path}?{urlencode(query)}"


def _merge_event(base: Dict[str, Any], detail: Dict[str, Any]) -> Dict[str, Any]:
    merged = {**base}
    for key, value in detail.items():
        if value not in (None, "", [], {}):
            if key == "occurrence" and isinstance(value, dict):
                merged[key] = {**(base.get("occurrence") or {}), **value}
                merged[key] = {k: v for k, v in merged[key].items() if v not in (None, "", [], {})}
            else:
                merged[key] = value
    metadata = {**(base.get("metadata") or {}), **(detail.get("metadata") or {})}
    if metadata:
        merged["metadata"] = metadata
    return merged


def _parse_detail_event(html: str, source_url: str) -> Dict[str, Any]:
    json_ld_events = _extract_json_ld_events(html, source_url)
    if json_ld_events:
        return _json_ld_to_event(json_ld_events[0], source_url)

    soup = BeautifulSoup(html or "", "html.parser")
    region = _main_detail_region(soup)
    title = _clean_text((region.find("h1") or soup.find("h1") or soup.find("title") or region).get_text(" ", strip=True))
    text = _clean_text(region.get_text(" ", strip=True)) or ""
    date_line = _extract_date_line(region)
    start_at, end_at, display_date = _parse_date_range(date_line or text[:500])
    description = _read_meta(soup, "description", "og:description") or _extract_description(region, title or "", date_line, None)
    image_url = _read_meta(soup, "og:image", "twitter:image")
    expected_attendance = _parse_attendance(text)
    location_text = _extract_location_from_scope(region, title or "", date_line)
    city, country = _parse_city_country(location_text)
    event_type = None
    for category in _extract_categories(region):
        if re.search(r"conference|trade show|expo|workshop|convention|seminar|training", category, re.I):
            event_type = category
            break

    return {
        "name": title,
        "url": source_url,
        "sourceUrl": source_url,
        "sourceProvider": "10times",
        "eventType": event_type or "Conference",
        "startDate": start_at,
        "endDate": end_at or start_at,
        "city": city,
        "country": country,
        "description": _clean_text(description) or text[:1200],
        "eventImageUrl": image_url,
        "expectedAttendance": expected_attendance,
        "categories": _extract_categories(region),
        "occurrence": {
            key: value
            for key, value in {
                "locationText": location_text,
                "city": city,
                "country": country,
                "expectedAttendance": expected_attendance,
            }.items()
            if value not in (None, "", [], {})
        },
        "metadata": {
            "source": "10times",
            "displayStartAt": display_date,
            "detailText": text[:3000],
        },
    }


def _first_string(*values: Any) -> Optional[str]:
    for value in values:
        if isinstance(value, str) and value.strip():
            return value.strip()
        if isinstance(value, (int, float)) and not isinstance(value, bool):
            return str(value)
    return None


def _first_float(*values: Any) -> Optional[float]:
    for value in values:
        try:
            if value is not None and str(value).strip() != "":
                return float(value)
        except (TypeError, ValueError):
            continue
    return None


def _map_10times_event_to_generic(raw_event: Dict[str, Any]) -> Optional[GenericMappedEventDict]:
    name = _first_string(raw_event.get("name"), raw_event.get("title"))
    if not name:
        return None

    location = raw_event.get("location") if isinstance(raw_event.get("location"), dict) else {}
    address = location.get("address") if isinstance(location.get("address"), dict) else {}
    raw_occurrence = raw_event.get("occurrence") if isinstance(raw_event.get("occurrence"), dict) else {}
    organizer = raw_event.get("organizer") if isinstance(raw_event.get("organizer"), dict) else {}
    enriched = raw_event.get("enriched_details") if isinstance(raw_event.get("enriched_details"), dict) else {}

    city = _first_string(
        address.get("addressLocality"),
        raw_event.get("city"),
        raw_event.get("locationText"),
    )
    country = _first_string(address.get("addressCountry"), raw_event.get("country"))
    venue_name = _first_string(location.get("name"), raw_event.get("venueName"), raw_occurrence.get("venueName"), enriched.get("venue_name"))
    street_address = _first_string(address.get("streetAddress"), raw_occurrence.get("streetAddress"), enriched.get("venue_address"))

    speakers = enriched.get("speakers") if isinstance(enriched.get("speakers"), list) else []
    attendees: List[GenericAttendeeDict] = []
    for speaker in speakers:
        if not isinstance(speaker, dict):
            continue
        speaker_name = _first_string(speaker.get("name"))
        if not speaker_name:
            continue
        attendees.append(
            {
                "fullName": speaker_name,
                "title": _first_string(speaker.get("role")),
                "linkedInUrl": _first_string(speaker.get("linkedin")),
                "relationshipType": "SPEAKER",
                "metadataJson": {"sourceProvider": "10times", "speaker": speaker},
            }
        )

    sponsors = enriched.get("sponsors") if isinstance(enriched.get("sponsors"), list) else []
    companies: List[GenericCompanyDict] = []
    exhibitors = enriched.get("exhibitors") if isinstance(enriched.get("exhibitors"), list) else []
    for sponsor in [*sponsors, *exhibitors]:
        if not isinstance(sponsor, dict):
            continue
        sponsor_name = _first_string(sponsor.get("name"))
        if not sponsor_name:
            continue
        relationship = _first_string(
            sponsor.get("relationshipType"),
            sponsor.get("tier"),
            "Exhibitor" if sponsor in exhibitors else "Sponsor",
        )
        companies.append(
            {
                "name": sponsor_name,
                "websiteUrl": _first_string(sponsor.get("website")),
                "relationshipType": relationship,
                "metadataJson": {"sourceProvider": "10times", "sponsor": sponsor},
            }
        )

    occurrence: GenericOccurrenceDict = {
        "locationText": _first_string(raw_event.get("locationText"), raw_occurrence.get("locationText"), venue_name, street_address, city),
        "latitude": _first_float(location.get("latitude"), raw_event.get("latitude"), raw_occurrence.get("latitude"), enriched.get("latitude")),
        "longitude": _first_float(location.get("longitude"), raw_event.get("longitude"), raw_occurrence.get("longitude"), enriched.get("longitude")),
        "venueName": venue_name,
        "streetAddress": street_address,
        "city": city,
        "region": _first_string(address.get("addressRegion")),
        "postalCode": _first_string(address.get("postalCode")),
        "country": country,
        "timezone": _first_string(raw_event.get("timezone")),
    }
    occurrence = {key: value for key, value in occurrence.items() if value not in (None, "", [], {})}  # type: ignore

    categories = raw_event.get("categories") if isinstance(raw_event.get("categories"), list) else raw_event.get("keywords")
    category = _first_string(*(categories or [])) if isinstance(categories, list) else _first_string(categories)

    metadata = {
        "sourceProvider": "10times",
        "source": "10times",
        "enriched_details": enriched,
    }
    metadata = {key: value for key, value in metadata.items() if value not in (None, "", [], {})}

    event: GenericMappedEventDict = {
        "name": name,
        "sourceUrl": _first_string(raw_event.get("sourceUrl"), raw_event.get("url")),
        "startAt": _first_string(raw_event.get("startAt"), raw_event.get("startDate")),
        "endAt": _first_string(raw_event.get("endAt"), raw_event.get("endDate")),
        "city": city,
        "country": country,
        "eventType": _first_string(raw_event.get("eventType"), category, "Conference"),
        "category": category,
        "organizerName": _first_string(organizer.get("name"), raw_event.get("organizerName")),
        "organizerWebsite": _first_string(organizer.get("url"), raw_event.get("organizerWebsite")),
        "eventImageUrl": _first_string(raw_event.get("image"), raw_event.get("eventImageUrl")),
        "industry": category,
        "description": _clean_text(raw_event.get("description")),
        "sourceProvider": "10times",
        "attendees": attendees,
        "companies": companies,
        "occurrence": occurrence,
        "metadataJson": metadata,
    }
    return {key: value for key, value in event.items() if value not in (None, "", [], {})}  # type: ignore


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
        "cookie_used": bool(cookie),
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

        current_page_html = html
        for page in range(2, max(1, pages) + 1):
            if len(events_by_url) >= limit:
                break
            ajax_url = _build_listing_ajax_url(list_url, page, current_page_html)
            if not ajax_url:
                failures.append({"url": list_url, "reason": "missing_10times_listing_pagination_ids", "page": page})
                break
            ajax_html, ajax_failures = await _fetch_listing_page(
                client,
                ajax_url,
                source=source,
                referer=list_url,
                cookie=cookie,
            )
            failures.extend(ajax_failures)
            if not ajax_html:
                continue
            diagnostics["list_pages_fetched"] += 1
            current_page_html = ajax_html
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
        "events": [mapped for event in events if (mapped := _map_10times_event_to_generic(event))],
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
        already_mapped=True,
    )
    response["diagnostics"] = diagnostics or {}
    return response
