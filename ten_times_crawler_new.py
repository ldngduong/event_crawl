from __future__ import annotations

import asyncio
import json
import logging
import re
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import parse_qsl, urlencode, urlparse, urlunparse

import httpx
from bs4 import BeautifulSoup

from generic_mapper import ingest_generic_events_to_eagle
from schemas import GenericMappedEventDict
from ten_minute_mail import TemporaryEmailOtp, TenMinuteMailClient
from ten_times_crawler import (
    TEN_TIMES_BASE_URL,
    TEN_TIMES_DEFAULT_LIST_URL,
    _ajax_headers,
    _build_listing_ajax_url,
    _dedupe_events,
    _headers,
    _is_wall_html,
    _map_10times_event_to_generic,
    _merge_event,
    _parse_detail_event,
    _parse_list_events,
)

logger = logging.getLogger(__name__)

DEFAULT_COOKIE_PATH = Path(__file__).resolve().parent / ".ten_times_cookies.json"


@dataclass
class TenTimesLoginResult:
    ok: bool
    status_code: int
    url: str
    message: Optional[str]
    cookies: Dict[str, str]
    body_preview: str


@dataclass
class TenTimesTemporaryEmailLoginResult:
    email: str
    email_seconds_left: int
    otp: Optional[str]
    otp_message: Optional[Dict[str, Any]]
    otp_request: TenTimesLoginResult
    verify: Optional[TenTimesLoginResult]
    cookie_header: str
    post_login_url: Optional[str] = None
    event_api_calls: Optional[List[Dict[str, Any]]] = None


class TenTimesSessionClient:
    """
    Direct 10times client that keeps cookies in one httpx session.

    Login flow observed from the browser:
    - request OTP: POST https://10times.com/login with form field email1
    - verify OTP: POST https://10times.com/login with email/userEmail/otp1..otp4/OTP

    10times is behind Cloudflare. If plain HTTP gets the challenge page, call
    bootstrap_cloudflare_cookies() once, then retry the API flow with the saved
    cf_clearance/session cookies.
    """

    def __init__(
        self,
        cookie_path: Path | str = DEFAULT_COOKIE_PATH,
        timeout: float = 45.0,
        headers: Optional[Dict[str, str]] = None,
    ) -> None:
        self.cookie_path = Path(cookie_path)
        self._client = httpx.AsyncClient(
            timeout=timeout,
            follow_redirects=True,
            headers={
                **_headers(referer=f"{TEN_TIMES_BASE_URL}/login"),
                "Origin": TEN_TIMES_BASE_URL,
                "Content-Type": "application/x-www-form-urlencoded",
                **(headers or {}),
            },
        )

    async def __aenter__(self) -> "TenTimesSessionClient":
        await self.load_cookies()
        return self

    async def __aexit__(self, *_: object) -> None:
        await self.aclose()

    async def aclose(self) -> None:
        await self._client.aclose()

    async def load_cookies(self) -> None:
        if not self.cookie_path.exists():
            return
        data = json.loads(self.cookie_path.read_text(encoding="utf-8"))
        for cookie in data.get("cookies", []):
            name = cookie.get("name")
            value = cookie.get("value")
            if name and value is not None:
                self._client.cookies.set(
                    name,
                    value,
                    domain=cookie.get("domain") or "10times.com",
                    path=cookie.get("path") or "/",
                )

    async def save_cookies(self) -> None:
        cookies = []
        for cookie in self._client.cookies.jar:
            cookies.append(
                {
                    "name": cookie.name,
                    "value": cookie.value,
                    "domain": cookie.domain,
                    "path": cookie.path,
                    "expires": cookie.expires,
                }
            )
        self.cookie_path.write_text(
            json.dumps({"savedAt": time.time(), "cookies": cookies}, indent=2),
            encoding="utf-8",
        )

    async def clear_cookies(self) -> None:
        self._client.cookies.clear()
        if self.cookie_path.exists():
            self.cookie_path.unlink()

    def cookie_header(self) -> str:
        return "; ".join(f"{cookie.name}={cookie.value}" for cookie in self._client.cookies.jar)

    async def bootstrap_cloudflare_cookies(
        self,
        url: str = f"{TEN_TIMES_BASE_URL}/login",
        headless: bool = False,
        wait_seconds: float = 8.0,
    ) -> Dict[str, str]:
        from playwright.async_api import async_playwright

        async with async_playwright() as p:
            browser = await p.chromium.launch(
                headless=headless,
                args=["--disable-blink-features=AutomationControlled"],
            )
            context = await browser.new_context(viewport={"width": 1365, "height": 900})
            page = await context.new_page()
            await page.goto(url, wait_until="domcontentloaded", timeout=90000)
            await _auto_tick_cloudflare(page)
            await page.wait_for_timeout(int(wait_seconds * 1000))
            cookies = await context.cookies(TEN_TIMES_BASE_URL)
            await browser.close()

        for cookie in cookies:
            self._client.cookies.set(
                cookie["name"],
                cookie["value"],
                domain=cookie.get("domain") or "10times.com",
                path=cookie.get("path") or "/",
            )
        await self.save_cookies()
        return {cookie["name"]: cookie["value"] for cookie in cookies}

    async def request_login_otp(self, email: str) -> TenTimesLoginResult:
        response = await self._client.post(
            f"{TEN_TIMES_BASE_URL}/login",
            data={"email1": email},
            headers=_form_headers(referer=f"{TEN_TIMES_BASE_URL}/login"),
        )
        await self.save_cookies()
        return _login_result(response)

    async def verify_login_otp(
        self,
        email: str,
        otp: str,
        city: str = "",
    ) -> TenTimesLoginResult:
        digits = list(str(otp).strip())
        if len(digits) != 4 or not all(digit.isdigit() for digit in digits):
            raise ValueError("10times OTP must be exactly 4 digits")

        response = await self._client.post(
            f"{TEN_TIMES_BASE_URL}/login",
            data={
                "email": email,
                "city": city,
                "userEmail": email,
                "otp1": digits[0],
                "otp2": digits[1],
                "otp3": digits[2],
                "otp4": digits[3],
                "OTP": "".join(digits),
            },
            headers=_form_headers(referer=f"{TEN_TIMES_BASE_URL}/login"),
        )
        await self.save_cookies()
        return _login_result(response)

    async def login_with_otp(
        self,
        email: str,
        otp: str,
        bootstrap_cloudflare: bool = True,
    ) -> Dict[str, Any]:
        otp_request = await self.request_login_otp(email)
        if bootstrap_cloudflare and _looks_blocked(otp_request.body_preview):
            await self.bootstrap_cloudflare_cookies()
            otp_request = await self.request_login_otp(email)

        verify_result = await self.verify_login_otp(email, otp)
        return {
            "otp_request": otp_request.__dict__,
            "verify": verify_result.__dict__,
            "cookie_header": self.cookie_header(),
        }

    async def login_with_temporary_email(
        self,
        mail_client: Optional[TenMinuteMailClient] = None,
        *,
        bootstrap_cloudflare: bool = True,
        force_new_email: bool = True,
        otp_timeout_seconds: float = 90.0,
        otp_poll_interval_seconds: float = 30.0,
    ) -> TenTimesTemporaryEmailLoginResult:
        owns_mail_client = mail_client is None
        mail = mail_client or TenMinuteMailClient(preferred_domain_suffix=".net")

        try:
            if owns_mail_client:
                await mail.__aenter__()

            temporary_email = await mail.get_gmail(force_new=force_new_email)
            otp_request = await self.request_login_otp(temporary_email.address)
            if bootstrap_cloudflare and _looks_blocked(otp_request.body_preview):
                await self.bootstrap_cloudflare_cookies()
                otp_request = await self.request_login_otp(temporary_email.address)

            otp_result = await _wait_for_10times_otp(
                mail,
                timeout_seconds=otp_timeout_seconds,
                poll_interval_seconds=otp_poll_interval_seconds,
            )
            if otp_result is None:
                return TenTimesTemporaryEmailLoginResult(
                    email=temporary_email.address,
                    email_seconds_left=temporary_email.seconds_left,
                    otp=None,
                    otp_message=None,
                    otp_request=otp_request,
                    verify=None,
                    cookie_header=self.cookie_header(),
                )

            verify_result = await self.verify_login_otp(
                temporary_email.address,
                otp_result.otp,
            )
            return TenTimesTemporaryEmailLoginResult(
                email=temporary_email.address,
                email_seconds_left=temporary_email.seconds_left,
                otp=otp_result.otp,
                otp_message=otp_result.message,
                otp_request=otp_request,
                verify=verify_result,
                cookie_header=self.cookie_header(),
            )
        finally:
            if owns_mail_client:
                await mail.__aexit__(None, None, None)

    async def login_with_temporary_email_browser(
        self,
        mail_client: Optional[TenMinuteMailClient] = None,
        *,
        headless: bool = False,
        force_new_email: bool = True,
        otp_timeout_seconds: float = 180.0,
        otp_poll_interval_seconds: float = 30.0,
        post_login_url: str = TEN_TIMES_DEFAULT_LIST_URL,
        listen_event_api: bool = True,
    ) -> TenTimesTemporaryEmailLoginResult:
        from playwright.async_api import async_playwright

        owns_mail_client = mail_client is None
        mail = mail_client or TenMinuteMailClient(preferred_domain_suffix=".net")
        otp_request = TenTimesLoginResult(
            ok=False,
            status_code=0,
            url=f"{TEN_TIMES_BASE_URL}/login",
            message=None,
            cookies={},
            body_preview="",
        )

        try:
            if owns_mail_client:
                await mail.__aenter__()

            temporary_email = await mail.get_gmail(force_new=force_new_email)

            async with async_playwright() as p:
                browser = await p.chromium.launch(
                    headless=headless,
                    args=["--disable-blink-features=AutomationControlled"],
                )
                context = await browser.new_context(viewport={"width": 1365, "height": 900})
                page = await context.new_page()
                event_api_calls: List[Dict[str, Any]] = []
                if listen_event_api:
                    _attach_event_api_listener(page, event_api_calls)

                await page.goto(f"{TEN_TIMES_BASE_URL}/login", wait_until="domcontentloaded", timeout=90000)
                await _auto_tick_cloudflare(page)
                await page.wait_for_timeout(8000)
                email_input = page.locator("#valEmail, input[name='email1'], input[placeholder*='Email']").first
                await email_input.fill(temporary_email.address)
                submit = page.locator("#send, input[type=submit][value='Submit']").first
                await submit.click()
                await page.wait_for_timeout(3000)
                otp_request = TenTimesLoginResult(
                    ok="sent a 4 digit OTP" in await page.locator("body").inner_text(),
                    status_code=200,
                    url=page.url,
                    message=_clean_text((await page.locator("body").inner_text())[:500]),
                    cookies={},
                    body_preview=_clean_text((await page.locator("body").inner_text())[:1200]) or "",
                )

                otp_result = await _wait_for_10times_otp(
                    mail,
                    timeout_seconds=otp_timeout_seconds,
                    poll_interval_seconds=otp_poll_interval_seconds,
                )
                if otp_result is None:
                    cookies = await context.cookies(TEN_TIMES_BASE_URL)
                    await self._store_playwright_cookies(cookies)
                    await browser.close()
                    return TenTimesTemporaryEmailLoginResult(
                        email=temporary_email.address,
                        email_seconds_left=temporary_email.seconds_left,
                        otp=None,
                        otp_message=None,
                        otp_request=otp_request,
                        verify=None,
                        cookie_header=self.cookie_header(),
                        post_login_url=page.url,
                        event_api_calls=event_api_calls,
                    )

                digits = list(otp_result.otp)
                for selector, digit in zip(("#otp1", "#otp2", "#otp3", "#otp4"), digits):
                    await page.locator(selector).fill(digit)
                verify_submit = page.locator(
                    "input[type=submit][value*='Verify'], button:has-text('Verify')"
                ).first
                await verify_submit.click()
                await page.wait_for_timeout(6000)

                body_text = await page.locator("body").inner_text()
                post_login_final_url = page.url
                if post_login_url:
                    await page.goto(post_login_url, wait_until="domcontentloaded", timeout=90000)
                    await page.wait_for_timeout(5000)
                    await _scroll_for_event_api(page)
                    post_login_final_url = page.url

                cookies = await context.cookies(TEN_TIMES_BASE_URL)
                await self._store_playwright_cookies(cookies)
                await browser.close()

                verify_result = TenTimesLoginResult(
                    ok="valid login code" not in body_text.lower() and "Please enter your OTP" not in body_text,
                    status_code=200,
                    url=page.url,
                    message=_clean_text(body_text[:500]),
                    cookies={cookie["name"]: cookie["value"] for cookie in cookies},
                    body_preview=_clean_text(body_text[:1200]) or "",
                )
                return TenTimesTemporaryEmailLoginResult(
                    email=temporary_email.address,
                    email_seconds_left=temporary_email.seconds_left,
                    otp=otp_result.otp,
                    otp_message=otp_result.message,
                    otp_request=otp_request,
                    verify=verify_result,
                    cookie_header=self.cookie_header(),
                    post_login_url=post_login_final_url,
                    event_api_calls=event_api_calls,
                )
        finally:
            if owns_mail_client:
                await mail.__aexit__(None, None, None)

    async def _store_playwright_cookies(self, cookies: List[Dict[str, Any]]) -> None:
        for cookie in cookies:
            self._client.cookies.set(
                cookie["name"],
                cookie["value"],
                domain=cookie.get("domain") or "10times.com",
                path=cookie.get("path") or "/",
            )
        await self.save_cookies()

    async def get_html(self, url: str, referer: Optional[str] = None) -> Tuple[Optional[str], Optional[Dict[str, Any]]]:
        try:
            response = await self._client.get(
                url,
                headers=_headers(referer=referer or TEN_TIMES_BASE_URL),
            )
            text = response.text
            await self.save_cookies()
            if response.status_code >= 400:
                return None, {
                    "url": url,
                    "reason": f"http_status:{response.status_code}",
                    "body_preview": text[:500],
                }
            if _is_wall_html(text):
                return None, {
                    "url": url,
                    "reason": "human_check_or_captcha_wall",
                    "body_preview": _clean_text(text[:500]),
                }
            if _looks_like_10times_limit_wall(text):
                return None, {
                    "url": url,
                    "reason": "daily_limit_wall",
                    "body_preview": _clean_text(text[:500]),
                }
            return text, None
        except Exception as error:
            return None, {"url": url, "reason": "direct_fetch_exception", "error": str(error)}

    async def get_listing_ajax_html(
        self,
        url: str,
        referer: str,
    ) -> Tuple[Optional[str], Optional[Dict[str, Any]]]:
        try:
            response = await self._client.get(
                url,
                headers=_ajax_headers(referer, self.cookie_header()),
            )
            text = response.text
            await self.save_cookies()
            if response.status_code >= 400:
                return None, {
                    "url": url,
                    "reason": f"listing_ajax_status:{response.status_code}",
                    "body_preview": text[:500],
                }
            if _is_wall_html(text):
                return None, {
                    "url": url,
                    "reason": "listing_ajax_human_check_or_captcha_wall",
                    "body_preview": _clean_text(text[:500]),
                }
            if _looks_like_10times_limit_wall(text):
                return None, {
                    "url": url,
                    "reason": "daily_limit_wall",
                    "body_preview": _clean_text(text[:500]),
                }
            return text, None
        except Exception as error:
            return None, {"url": url, "reason": "listing_ajax_fetch_exception", "error": str(error)}


async def crawl_ten_times_events_with_diagnostics_new(
    *,
    list_url: str = TEN_TIMES_DEFAULT_LIST_URL,
    limit: int = 50,
    pages: int = 1,
    max_pages: int = 100,
    enrich_details: bool = True,
    cookie_path: Path | str = DEFAULT_COOKIE_PATH,
    bootstrap_cloudflare: bool = False,
    refresh_login_on_block: bool = True,
    login_headless: bool = False,
) -> Dict[str, Any]:
    all_pages = pages <= 0
    effective_pages = max_pages if all_pages else max(1, pages)
    diagnostics: Dict[str, Any] = {
        "source": "html_session",
        "list_url": list_url,
        "pages_requested": pages,
        "max_pages": max_pages,
        "all_pages": all_pages,
        "brightdata": False,
        "cookie_path": str(cookie_path),
        "list_pages_fetched": 0,
        "detail_pages_fetched": 0,
        "login_refresh_triggered": False,
        "page_results": [],
    }
    failures: List[Dict[str, Any]] = []
    parse_failures: List[Dict[str, Any]] = []
    events_by_url: Dict[str, Dict[str, Any]] = {}

    async with TenTimesSessionClient(cookie_path=cookie_path) as session:
        if bootstrap_cloudflare:
            await session.bootstrap_cloudflare_cookies(url=list_url)

        html, failure = await session.get_html(list_url)
        if failure and refresh_login_on_block and _should_refresh_10times_login(failure):
            diagnostics["login_refresh_triggered"] = True
            try:
                await session.clear_cookies()
                login_result = await session.login_with_temporary_email_browser(
                    headless=login_headless,
                    post_login_url=list_url,
                    listen_event_api=False,
                )
                diagnostics["login_refresh"] = {
                    "email": login_result.email,
                    "otp_received": bool(login_result.otp),
                    "otp_request_ok": login_result.otp_request.ok,
                    "verify_ok": bool(login_result.verify and login_result.verify.ok),
                    "post_login_url": login_result.post_login_url,
                }
                html, retry_failure = await session.get_html(list_url)
                if retry_failure:
                    failures.append({**failure, "source": "session_html_before_login_refresh"})
                    failure = retry_failure
                else:
                    failure = None
            except Exception as error:
                diagnostics["login_refresh_error"] = str(error)

        if failure:
            failures.append({**failure, "source": "session_html"})
        if not html:
            return {"events": [], "parse_failures": failures, "diagnostics": diagnostics}

        diagnostics["list_pages_fetched"] += 1
        list_events, detail_urls = _parse_list_events(html, list_url)
        for event in list_events:
            if event.get("sourceUrl"):
                events_by_url[event["sourceUrl"]] = event
        diagnostics["page_results"].append(
            {
                "page": 1,
                "source": "session_html",
                "parsed_events": len(list_events),
                "new_events": len(events_by_url),
                "total_events": len(events_by_url),
            }
        )

        current_page_html = html
        for page in range(2, effective_pages + 1):
            if len(events_by_url) >= limit:
                diagnostics["stop_reason"] = "limit_reached"
                break

            page_url = _build_listing_page_url(list_url, page)
            ajax_url = _build_listing_ajax_url(list_url, page, current_page_html)
            if ajax_url:
                page_html, page_failure = await session.get_listing_ajax_html(ajax_url, referer=list_url)
                page_source = "session_listing_ajax"
            else:
                page_html = None
                page_failure = {
                    "url": list_url,
                    "reason": "missing_10times_listing_pagination_ids",
                    "page": page,
                }
                page_source = "session_listing_ajax"

            if page_failure or not page_html:
                failures.append({**page_failure, "source": page_source, "page": page})
                page_html, page_failure = await session.get_html(page_url, referer=list_url)
                page_source = "session_html_page"

            if page_failure:
                failures.append({**page_failure, "source": page_source, "page": page})
                continue
            if not page_html:
                continue

            diagnostics["list_pages_fetched"] += 1
            page_events, page_detail_urls = _parse_list_events(page_html, page_url)
            detail_urls.extend(page_detail_urls)
            before_count = len(events_by_url)
            for event in page_events:
                if event.get("sourceUrl"):
                    events_by_url.setdefault(event["sourceUrl"], event)
            new_count = len(events_by_url) - before_count
            diagnostics["page_results"].append(
                {
                    "page": page,
                    "source": page_source,
                    "url": page_url,
                    "parsed_events": len(page_events),
                    "new_events": new_count,
                    "total_events": len(events_by_url),
                }
            )
            if _build_listing_ajax_url(list_url, page + 1, page_html):
                current_page_html = page_html
            if all_pages and len(events_by_url) == before_count:
                diagnostics["stop_reason"] = "no_new_events_on_page"
                diagnostics["stop_page"] = page
                break

        detail_urls = list(dict.fromkeys(detail_urls))[:limit]
        if enrich_details and detail_urls:
            semaphore = asyncio.Semaphore(3)

            async def fetch_detail(url: str) -> None:
                async with semaphore:
                    detail_html, detail_failure = await session.get_html(url, referer=list_url)
                    if detail_failure:
                        failures.append({**detail_failure, "source": "session_detail"})
                        return
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


async def crawl_ten_times_events_with_playwright_new(
    *,
    list_url: str = TEN_TIMES_DEFAULT_LIST_URL,
    limit: int = 50,
    pages: int = 1,
    max_pages: int = 100,
    enrich_details: bool = True,
    cookie_path: Path | str = DEFAULT_COOKIE_PATH,
    headless: bool = False,
    refresh_account_on_wall: bool = True,
    cloudflare_manual_wait_seconds: float = 180.0,
) -> Dict[str, Any]:
    from playwright.async_api import async_playwright

    all_pages = pages <= 0
    effective_pages = max_pages if all_pages else max(1, pages)
    diagnostics: Dict[str, Any] = {
        "source": "playwright",
        "list_url": list_url,
        "pages_requested": pages,
        "max_pages": max_pages,
        "all_pages": all_pages,
        "cookie_path": str(cookie_path),
        "list_pages_fetched": 0,
        "detail_pages_fetched": 0,
        "page_results": [],
        "account_refresh_triggered": False,
        "account_refreshes": [],
        "cloudflare_manual_waits": [],
    }
    failures: List[Dict[str, Any]] = []
    parse_failures: List[Dict[str, Any]] = []
    events_by_url: Dict[str, Dict[str, Any]] = {}
    detail_urls: List[str] = []

    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=headless,
            args=["--disable-blink-features=AutomationControlled"],
        )
        context = await browser.new_context(viewport={"width": 1365, "height": 900})
        await _load_playwright_cookie_file(context, cookie_path)
        page = await context.new_page()
        account_refresh_count = 0
        max_account_refreshes = 10

        async def wait_for_manual_cloudflare(target_page: Any, *, source: str, url: str) -> bool:
            if headless or cloudflare_manual_wait_seconds <= 0:
                return False
            started_at = time.time()
            diagnostics["cloudflare_manual_waits"].append(
                {
                    "source": source,
                    "url": url,
                    "timeout_seconds": cloudflare_manual_wait_seconds,
                }
            )
            while time.time() - started_at < cloudflare_manual_wait_seconds:
                await _auto_tick_cloudflare(target_page)
                await target_page.wait_for_timeout(3000)
                html_after_wait = await target_page.content()
                if not _looks_like_10times_security_verification(html_after_wait):
                    await _save_playwright_cookie_file(context, cookie_path)
                    diagnostics["cloudflare_manual_waits"][-1]["resolved"] = True
                    diagnostics["cloudflare_manual_waits"][-1]["waited_seconds"] = round(time.time() - started_at, 1)
                    return True
            diagnostics["cloudflare_manual_waits"][-1]["resolved"] = False
            diagnostics["cloudflare_manual_waits"][-1]["waited_seconds"] = round(time.time() - started_at, 1)
            return False

        async def refresh_browser_session() -> bool:
            nonlocal browser, context, page, account_refresh_count
            if not refresh_account_on_wall or account_refresh_count >= max_account_refreshes:
                return False
            diagnostics["account_refresh_triggered"] = True
            try:
                login_result = await _refresh_10times_account_with_browser(
                    cookie_path=cookie_path,
                    list_url=list_url,
                    headless=headless,
                )
                account_refresh_count += 1
                refresh_record = {
                    "index": account_refresh_count,
                    "email": login_result.email,
                    "otp_received": bool(login_result.otp),
                    "otp_request_ok": login_result.otp_request.ok,
                    "verify_ok": bool(login_result.verify and login_result.verify.ok),
                    "post_login_url": login_result.post_login_url,
                }
                diagnostics["account_refreshes"].append(refresh_record)
                diagnostics["account_refresh"] = refresh_record
                await browser.close()
                browser = await p.chromium.launch(
                    headless=headless,
                    args=["--disable-blink-features=AutomationControlled"],
                )
                context = await browser.new_context(viewport={"width": 1365, "height": 900})
                await _load_playwright_cookie_file(context, cookie_path)
                page = await context.new_page()
                return True
            except Exception as error:
                diagnostics["account_refresh_error"] = str(error)
                return False

        async def goto_listing_page(target_page: int) -> bool:
            await page.goto(list_url, wait_until="domcontentloaded", timeout=90000)
            if target_page <= 1:
                return True
            for candidate_page in range(2, target_page + 1):
                clicked = await _click_listing_page_number(page, candidate_page)
                if not clicked:
                    return False
            return True

        page_number = 1
        try:
            await page.goto(list_url, wait_until="domcontentloaded", timeout=90000)
        except Exception as error:
            failures.append(
                {
                    "url": list_url,
                    "page": page_number,
                    "source": "playwright_page",
                    "reason": "playwright_page_exception",
                    "error": str(error),
                }
            )

        while not failures or diagnostics["list_pages_fetched"] > 0:
            if page_number > effective_pages:
                diagnostics["stop_reason"] = "max_pages_reached"
                break
            if len(events_by_url) >= limit:
                diagnostics["stop_reason"] = "limit_reached"
                break

            page_url = page.url
            try:
                await page.wait_for_timeout(2500)
                await _scroll_for_event_api(page)
                html = await page.content()
                await _save_playwright_cookie_file(context, cookie_path)
            except Exception as error:
                failures.append(
                    {
                        "url": page_url,
                        "page": page_number,
                        "source": "playwright_page",
                        "reason": "playwright_page_exception",
                        "error": str(error),
                    }
                )
                continue

            if _is_wall_html(html) or _looks_like_10times_limit_wall(html):
                if _looks_like_10times_security_verification(html) and await wait_for_manual_cloudflare(
                    page,
                    source="playwright_page",
                    url=page_url,
                ):
                    continue
                if await refresh_browser_session():
                    try:
                        navigated = await goto_listing_page(page_number)
                        if not navigated:
                            diagnostics["stop_reason"] = "cannot_restore_listing_page_after_account_refresh"
                            break
                        continue
                    except Exception as error:
                        diagnostics["account_refresh_error"] = str(error)
                failures.append(
                    {
                        "url": page_url,
                        "page": page_number,
                        "source": "playwright_page",
                        "reason": "wall_or_daily_limit",
                        "body_preview": _clean_text(html[:500]),
                    }
                )
                if page_number == 1:
                    break
                continue

            diagnostics["list_pages_fetched"] += 1
            page_events, page_detail_urls = _parse_list_events(html, page_url)
            detail_urls.extend(page_detail_urls)
            before_count = len(events_by_url)
            for event in page_events:
                if event.get("sourceUrl"):
                    events_by_url.setdefault(event["sourceUrl"], event)
            new_count = len(events_by_url) - before_count
            diagnostics["page_results"].append(
                {
                    "page": page_number,
                    "source": "playwright_page",
                    "url": page_url,
                    "parsed_events": len(page_events),
                    "new_events": new_count,
                    "total_events": len(events_by_url),
                }
            )

            next_page = page_number + 1
            if next_page > effective_pages:
                diagnostics["stop_reason"] = "max_pages_reached"
                break
            clicked_next = await _click_listing_page_number(page, next_page)
            if not clicked_next:
                diagnostics["stop_reason"] = "no_next_pagination_button"
                diagnostics["stop_page"] = page_number
                break
            page_number = next_page

        detail_urls = list(dict.fromkeys(detail_urls))[:limit]
        if enrich_details and detail_urls:
            detail_page = await context.new_page()
            for url in detail_urls:
                if diagnostics["detail_pages_fetched"] >= limit:
                    break
                detail_html = ""
                for attempt in range(2):
                    try:
                        await detail_page.goto(url, wait_until="domcontentloaded", timeout=90000)
                        await detail_page.wait_for_timeout(1200)
                        detail_html = await detail_page.content()
                    except Exception as error:
                        failures.append(
                            {
                                "url": url,
                                "source": "playwright_detail",
                                "reason": "playwright_detail_exception",
                                "error": str(error),
                            }
                        )
                        detail_html = ""
                        break
                    if not (_is_wall_html(detail_html) or _looks_like_10times_limit_wall(detail_html)):
                        break
                    if _looks_like_10times_security_verification(detail_html) and await wait_for_manual_cloudflare(
                        detail_page,
                        source="playwright_detail",
                        url=url,
                    ):
                        detail_html = await detail_page.content()
                        if not (_is_wall_html(detail_html) or _looks_like_10times_limit_wall(detail_html)):
                            break
                    if attempt == 0 and await refresh_browser_session():
                        await detail_page.close()
                        detail_page = await context.new_page()
                        continue
                    failures.append(
                        {
                            "url": url,
                            "source": "playwright_detail",
                            "reason": "wall_or_daily_limit",
                            "body_preview": _clean_text(detail_html[:500]),
                        }
                    )
                    detail_html = ""
                    break
                if not detail_html:
                    continue
                diagnostics["detail_pages_fetched"] += 1
                detail_event = _parse_detail_event(detail_html, url)
                detail_event = _merge_event(
                    detail_event,
                    _extract_10times_detail_enrichment(detail_html),
                )
                extra_detail_event = await _fetch_10times_detail_tabs(detail_page, url)
                if extra_detail_event:
                    detail_event = _merge_event(detail_event, extra_detail_event)
                if url in events_by_url:
                    events_by_url[url] = _merge_event(events_by_url[url], detail_event)
                else:
                    events_by_url[url] = detail_event
            await detail_page.close()

        await _save_playwright_cookie_file(context, cookie_path)
        await browser.close()

    events = _dedupe_events(list(events_by_url.values()))[:limit]
    if not events and not failures:
        parse_failures.append({"reason": "no_events_found", "list_url": list_url})

    return {
        "events": [mapped for event in events if (mapped := _map_10times_event_to_generic(event))],
        "parse_failures": [*parse_failures, *failures],
        "diagnostics": diagnostics,
    }


async def login_10times_with_temporary_email(
    *,
    cookie_path: Path | str = DEFAULT_COOKIE_PATH,
    bootstrap_cloudflare: bool = True,
    otp_timeout_seconds: float = 90.0,
    otp_poll_interval_seconds: float = 30.0,
) -> TenTimesTemporaryEmailLoginResult:
    async with TenTimesSessionClient(cookie_path=cookie_path) as ten_times:
        return await ten_times.login_with_temporary_email(
            bootstrap_cloudflare=bootstrap_cloudflare,
            otp_timeout_seconds=otp_timeout_seconds,
            otp_poll_interval_seconds=otp_poll_interval_seconds,
        )


async def ingest_ten_times_events_to_eagle_new(
    *,
    organization_id: Optional[str],
    workspace_id: Optional[str],
    events: List[GenericMappedEventDict],
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


def _form_headers(referer: str) -> Dict[str, str]:
    return {
        **_headers(referer=referer),
        "Origin": TEN_TIMES_BASE_URL,
        "Content-Type": "application/x-www-form-urlencoded",
    }


def _build_listing_page_url(list_url: str, page: int) -> str:
    parsed = urlparse(list_url)
    query = dict(parse_qsl(parsed.query, keep_blank_values=True))
    query["page"] = str(page)
    return urlunparse(
        (
            parsed.scheme or "https",
            parsed.netloc or "10times.com",
            parsed.path or "/newyork-us/conferences",
            parsed.params,
            urlencode(query),
            parsed.fragment,
        )
    )


async def _load_playwright_cookie_file(context: Any, cookie_path: Path | str) -> None:
    path = Path(cookie_path)
    if not path.exists():
        return
    data = json.loads(path.read_text(encoding="utf-8"))
    cookies = []
    for cookie in data.get("cookies", []):
        name = cookie.get("name")
        value = cookie.get("value")
        if not name or value is None:
            continue
        item = {
            "name": name,
            "value": value,
            "domain": cookie.get("domain") or "10times.com",
            "path": cookie.get("path") or "/",
        }
        expires = cookie.get("expires")
        if isinstance(expires, (int, float)) and expires > 0:
            item["expires"] = expires
        cookies.append(item)
    if cookies:
        await context.add_cookies(cookies)


async def _save_playwright_cookie_file(context: Any, cookie_path: Path | str) -> None:
    cookies = await context.cookies(TEN_TIMES_BASE_URL)
    Path(cookie_path).write_text(
        json.dumps({"savedAt": time.time(), "cookies": cookies}, indent=2),
        encoding="utf-8",
    )


async def _click_listing_page_number(page: Any, page_number: int) -> bool:
    selector = "div.pagination span.btn"
    buttons = page.locator(selector)
    count = await buttons.count()
    target_text = str(page_number)
    for index in range(count):
        button = buttons.nth(index)
        try:
            text = _clean_text(await button.inner_text())
            class_name = await button.get_attribute("class") or ""
            if text != target_text or "current" in class_name:
                continue
            await button.scroll_into_view_if_needed(timeout=5000)
            await button.click(timeout=10000)
            await page.wait_for_timeout(4500)
            return True
        except Exception:
            continue
    return False


async def _fetch_10times_detail_tabs(page: Any, source_url: str) -> Dict[str, Any]:
    merged: Dict[str, Any] = {}
    base = source_url.rstrip("/")
    for tab in ("speakers", "exhibitors"):
        tab_url = f"{base}/{tab}"
        try:
            await page.goto(tab_url, wait_until="domcontentloaded", timeout=90000)
            await page.wait_for_timeout(2500)
            html = await page.content()
        except Exception:
            continue
        if _is_wall_html(html) or _looks_like_10times_limit_wall(html):
            continue
        merged = _merge_event(merged, _extract_10times_detail_enrichment(html))
    return merged


async def _refresh_10times_account_with_browser(
    *,
    cookie_path: Path | str,
    list_url: str,
    headless: bool,
) -> TenTimesTemporaryEmailLoginResult:
    async with TenTimesSessionClient(cookie_path=cookie_path) as session:
        await session.clear_cookies()
        return await session.login_with_temporary_email_browser(
            headless=headless,
            force_new_email=True,
            post_login_url=list_url,
            listen_event_api=False,
        )


def _should_refresh_10times_login(failure: Dict[str, Any]) -> bool:
    reason = str(failure.get("reason") or "").lower()
    preview = str(failure.get("body_preview") or "").lower()
    return (
        reason in {"human_check_or_captcha_wall"}
        or reason in {"daily_limit_wall", "not_event_listing_page"}
        or reason == "http_status:403"
        or "just a moment" in preview
        or "enable javascript and cookies" in preview
        or "security verification" in preview
        or "you're on a roll" in preview
        or "continue research on whr.ai" in preview
        or "daily limit resets" in preview
    )


def _looks_like_10times_limit_wall(html: str) -> bool:
    text = (html or "").lower()
    return (
        'id="pagetype" value="draft-login"' in text
        or "continue research on whr.ai" in text
        or "daily limit resets" in text
        or "you're on a roll" in text
    )


def _looks_like_10times_security_verification(html: str) -> bool:
    text = (html or "").lower()
    return (
        "performing security verification" in text
        or "security service to protect against malicious bots" in text
        or "verifies you are not a bot" in text
        or "challenge-platform" in text
        or "cf-mitigated" in text
        or "just a moment" in text
        or "enable javascript and cookies" in text
    )


def _extract_10times_detail_enrichment(html: str) -> Dict[str, Any]:
    soup = BeautifulSoup(html or "", "html.parser")
    body_text = soup.get_text("\n", strip=True)
    lines = [_clean_text(line) for line in body_text.splitlines()]
    lines = [line for line in lines if line]
    enriched: Dict[str, Any] = {}
    meta = _extract_detail_meta(soup)
    if meta:
        enriched["meta"] = meta
    data_layer = _extract_data_layer(html)
    if data_layer:
        enriched["data_layer"] = data_layer

    about = _section_text(lines, ("About",), ("Listed In", "Timings", "Entry Fees", "Estimated Turnout"))
    if about:
        enriched["about"] = about
    listed_in = _section_lines(lines, "Listed In", ("Excited about", "You are heading", "Share", "Timings"))
    categories = [line.lstrip("# ").strip() for line in listed_in if not line.lower().startswith("listed in")]
    categories = [
        line
        for line in categories
        if line and not re.search(r"^(share|excited about|you are heading|\d+\s+people\b)", line, re.I)
    ]
    if categories:
        enriched["listed_in"] = categories

    label_map = {
        "Timings": "timings",
        "Entry Fees": "entry_fees",
        "Estimated Turnout": "estimated_turnout",
        "Event Type": "event_type",
        "Editions": "editions",
        "Frequency": "frequency",
        "Official Links": "official_links",
    }
    for label, key in label_map.items():
        value = _section_text(
            lines,
            (label,),
            (
                "Entry Fees",
                "Estimated Turnout",
                "Event Type",
                "Editions",
                "Frequency",
                "Official Links",
                "Report Error",
                "Claim this event",
                "Organizer",
                "Venue",
                "Different Located Editions",
            ),
        )
        if value:
            enriched[key] = value

    organizer_lines = _section_lines(lines, "Organizer", ("Venue", "Plan your visit", "Visitor Ticket Price", "Frequently Asked Questions"))
    organizer = _parse_named_section_entity(
        organizer_lines,
        skip_markers=("Organizer", "Queries about", "Ask Organizer", "Send Stall", "Follow Company", "Renowned"),
    )
    if organizer:
        enriched["organizer"] = organizer

    venue_lines = _section_lines(lines, "Venue", ("Plan your visit", "Visitor Ticket Price", "Frequently Asked Questions"))
    venue = _parse_named_section_entity(
        venue_lines,
        skip_markers=("Venue", "Show interest", "address", "Get Directions"),
    )
    if venue:
        enriched["venue"] = venue
        if venue.get("name"):
            enriched["venue_name"] = venue["name"]
        if venue.get("location"):
            enriched["venue_address"] = venue["location"]
    coordinates = _extract_coordinates_before_venue(lines)
    if coordinates:
        enriched["latitude"] = coordinates[0]
        enriched["longitude"] = coordinates[1]

    speakers = _parse_people_section(lines, "Speakers", ("Interested", "More Speakers", "Sponsors", "You may also like"))
    if speakers:
        enriched["speakers"] = speakers

    sponsors = _parse_sponsors_section(lines, "Sponsors", ("View All Sponsors", "You may also like", "Followers"))
    if sponsors:
        enriched["sponsors"] = sponsors
    exhibitors = _parse_exhibitors(soup)
    if exhibitors:
        enriched["exhibitors"] = exhibitors
    faqs = _parse_faqs(lines)
    if faqs:
        enriched["faqs"] = faqs
    metadata = {"source": "10times", "enriched_details": enriched}
    detail_event: Dict[str, Any] = {
        "enriched_details": enriched,
        "metadata": metadata,
    }
    if categories:
        detail_event["categories"] = categories
    if enriched.get("event_type"):
        detail_event["eventType"] = str(enriched["event_type"]).split()[0]
    if organizer.get("name"):
        detail_event["organizerName"] = organizer["name"]
    if venue.get("name") or venue.get("location"):
        detail_event["occurrence"] = {
            "venueName": venue.get("name"),
            "locationText": venue.get("location") or venue.get("name"),
        }
    if coordinates:
        detail_event["latitude"] = coordinates[0]
        detail_event["longitude"] = coordinates[1]
        detail_event["occurrence"] = {
            **(detail_event.get("occurrence") or {}),
            "latitude": coordinates[0],
            "longitude": coordinates[1],
        }
    return detail_event


def _section_lines(lines: List[str], start_label: str, end_labels: Tuple[str, ...]) -> List[str]:
    start_index = _find_line_index(lines, start_label)
    if start_index is None:
        return []
    collected: List[str] = []
    for line in lines[start_index + 1 :]:
        if any(line == end or line.startswith(f"{end} ") for end in end_labels):
            break
        collected.append(line)
    return collected


def _extract_detail_meta(soup: BeautifulSoup) -> Dict[str, Any]:
    meta: Dict[str, Any] = {}
    title = soup.find("title")
    if title:
        meta["title"] = _clean_text(title.get_text(" ", strip=True))
    canonical = soup.find("link", rel=lambda value: value and "canonical" in value)
    if canonical and canonical.get("href"):
        meta["canonicalUrl"] = canonical.get("href")
    for node in soup.find_all("meta"):
        key = node.get("name") or node.get("property")
        value = node.get("content")
        if key and value and key in {
            "description",
            "og:title",
            "og:description",
            "og:url",
            "og:image",
            "twitter:title",
            "twitter:description",
            "twitter:image",
        }:
            meta[key] = value
    return meta


def _extract_data_layer(html: str) -> List[Dict[str, Any]]:
    match = re.search(r"dataLayer\s*=\s*(\[[\s\S]*?\])\s*</script>", html or "", re.I)
    if not match:
        return []
    try:
        data = json.loads(match.group(1))
        return data if isinstance(data, list) else []
    except Exception:
        return []


def _first_regex_int(text: str, pattern: str) -> Optional[int]:
    match = re.search(pattern, text or "", re.I | re.S)
    if not match:
        return None
    try:
        return int(match.group(1).replace(",", ""))
    except ValueError:
        return None


def _first_regex_text(text: str, pattern: str) -> Optional[str]:
    match = re.search(pattern, text or "", re.I | re.S)
    if not match:
        return None
    return _clean_text(match.group(1))


def _extract_coordinates_before_venue(lines: List[str]) -> Optional[Tuple[float, float]]:
    for index, line in enumerate(lines):
        if line != "Venue" or index < 2:
            continue
        try:
            latitude = float(lines[index - 2])
            longitude = float(lines[index - 1])
        except (TypeError, ValueError):
            continue
        if -90 <= latitude <= 90 and -180 <= longitude <= 180:
            return latitude, longitude
    return None


def _parse_label_counts(lines: List[str]) -> Dict[str, int]:
    counts: Dict[str, int] = {}
    for line in lines:
        match = re.match(r"(.+?)\s+\[(\d+)\]$", line)
        if match:
            counts[match.group(1).strip()] = int(match.group(2))
    return counts


def _parse_interested_profiles(lines: List[str]) -> List[Dict[str, str]]:
    section = _section_lines_from_best_match(lines, "Followers", ("Add Profile", "View More", "Speakers", "Sponsors", "You may also like"), "Recommended")
    if not section:
        section = _section_lines(lines, "Recommended", ("Add Profile", "View More", "Speakers", "Sponsors", "You may also like"))
    if "Recommended" in section:
        section = section[section.index("Recommended") + 1 :]
    profiles: List[Dict[str, str]] = []
    skip = {
        "Followers",
        "[ Users who have shown interest for this Event ]",
        "Join Community",
        "Invite",
        "All Profiles",
        "All Countries",
        "Sort By",
        "Top Profiles",
        "Recommended",
        "Connect",
    }
    useful = [line for line in section if line not in skip and not re.match(r".+\[\d+\]$", line)]
    index = 0
    while index < len(useful):
        name = useful[index]
        if not _looks_like_profile_name(name):
            index += 1
            continue
        profile: Dict[str, str] = {"name": name}
        if index + 1 < len(useful) and useful[index + 1] != "Connect":
            role_line = useful[index + 1]
            if " at " in role_line:
                role, organization = role_line.split(" at ", 1)
                profile["role"] = role.strip()
                profile["organization"] = organization.strip()
            else:
                profile["role"] = role_line
        if index + 2 < len(useful) and useful[index + 2] != "Connect":
            profile["location"] = useful[index + 2]
        profiles.append(profile)
        index += 3
    return profiles[:100]


def _looks_like_profile_name(value: str) -> bool:
    if not value or value in {"USA", "India", "UK"}:
        return False
    if re.search(r"(\[|\]|\bProfiles?\b|\bCountries\b|\bSort By\b)", value, re.I):
        return False
    return bool(re.search(r"[A-Za-z]", value)) and len(value.split()) <= 6


def _parse_faqs(lines: List[str]) -> List[Dict[str, str]]:
    section = _section_lines(lines, "Frequently Asked Questions", ("Edition", "How would you like", "Followers", "Speakers", "Sponsors", "You may also like"))
    faqs: List[Dict[str, str]] = []
    index = 0
    while index < len(section):
        question = section[index]
        if not question.endswith("?"):
            index += 1
            continue
        answer_parts: List[str] = []
        index += 1
        while index < len(section) and not section[index].endswith("?"):
            if section[index] not in {"Helpful", "Ask More Questions", "Contact Organizer"}:
                answer_parts.append(section[index])
            index += 1
        faqs.append({"question": question, "answer": _clean_text(" ".join(answer_parts)) or ""})
    return faqs


def _parse_related_events(lines: List[str]) -> List[Dict[str, str]]:
    section = _section_lines(lines, "Related Events", ("Featured Hotels", "More Hotels", "All Events", "Loading..."))
    events: List[Dict[str, str]] = []
    index = 0
    while index + 2 < len(section):
        if re.match(r"^[A-Z][a-z]{2}\s+\d{1,2}$", section[index]) and re.match(r"^\d{4}$", section[index + 1]):
            events.append(
                {
                    "date": f"{section[index]} {section[index + 1]}",
                    "name": section[index + 2],
                    "location": section[index + 3] if index + 3 < len(section) else "",
                }
            )
            index += 4
            continue
        index += 1
    return events[:50]


def _parse_featured_hotels(lines: List[str]) -> List[Dict[str, str]]:
    section = _section_lines(lines, "Featured Hotels in New York", ("More Hotels", "All Events", "Loading..."))
    hotels: List[Dict[str, str]] = []
    index = 0
    while index < len(section):
        name = section[index]
        price = section[index + 1] if index + 1 < len(section) and section[index + 1].lower().startswith("from ") else None
        if name and not name.lower().startswith("from "):
            item = {"name": name}
            if price:
                item["price"] = price
                index += 2
            else:
                index += 1
            hotels.append(item)
            continue
        index += 1
    return hotels[:30]


def _section_lines_from_best_match(
    lines: List[str],
    start_label: str,
    end_labels: Tuple[str, ...],
    required_marker: str,
) -> List[str]:
    indexes = [
        index
        for index, line in enumerate(lines)
        if line == start_label or line.startswith(f"{start_label} ")
    ]
    for index in reversed(indexes):
        window = lines[index + 1 : index + 120]
        if required_marker in window:
            collected: List[str] = []
            for line in lines[index + 1 :]:
                if any(line == end or line.startswith(f"{end} ") for end in end_labels):
                    break
                collected.append(line)
            return collected
    return _section_lines(lines, start_label, end_labels)


def _section_text(lines: List[str], start_labels: Tuple[str, ...], end_labels: Tuple[str, ...]) -> Optional[str]:
    for label in start_labels:
        section = _section_lines(lines, label, end_labels)
        if section:
            return _clean_text(" ".join(section))
    return None


def _find_line_index(lines: List[str], label: str) -> Optional[int]:
    for index, line in enumerate(lines):
        if line == label or line.startswith(f"{label} "):
            return index
    return None


def _parse_named_section_entity(lines: List[str], skip_markers: Tuple[str, ...]) -> Dict[str, str]:
    useful = [
        line
        for line in lines
        if line and not any(marker.lower() in line.lower() for marker in skip_markers)
    ]
    useful = [
        line
        for line in useful
        if not re.search(
            r"^(USA|United States|•|,|\d+|Total Events|\d+\s+Total Events|\d+\s+Followers|Followers|[-+]?\d+\.\d+|Copy Link|WhatsApp|Facebook|Twitter|LinkedIn|More Options|View All)$",
            line,
            re.I,
        )
    ]
    entity: Dict[str, str] = {}
    if useful:
        entity["name"] = useful[0]
    location_parts = [line for line in useful[1:4] if line not in {",", entity.get("name")}]
    if location_parts:
        entity["location"] = ", ".join(location_parts[:2])
    return entity


def _parse_people_section(lines: List[str], start_label: str, end_labels: Tuple[str, ...]) -> List[Dict[str, str]]:
    section = _section_lines_from_best_match(lines, start_label, end_labels, "Speaker")
    people: List[Dict[str, str]] = []
    index = 0
    while index < len(section):
        if section[index] == "Speaker" and index + 1 < len(section):
            person = {"name": section[index + 1]}
            if index + 2 < len(section) and section[index + 2] != "Follow":
                person["role"] = section[index + 2]
            people.append(person)
            index += 3
            continue
        index += 1
    return people


def _parse_sponsors_section(lines: List[str], start_label: str, end_labels: Tuple[str, ...]) -> List[Dict[str, str]]:
    section = _section_lines_from_best_match(lines, start_label, end_labels, "Follow")
    sponsors: List[Dict[str, str]] = []
    index = 0
    while index < len(section):
        name = section[index]
        if (
            name in {"Follow", "Sponsors"}
            or not name
            or name.startswith("http")
            or re.fullmatch(r"[-+]?\d+\.\d+", name)
        ):
            index += 1
            continue
        tier = section[index + 1] if index + 1 < len(section) else None
        if tier and tier != "Follow" and not re.fullmatch(r"[-+]?\d+\.\d+", tier):
            sponsors.append({"name": name, "tier": tier})
            index += 2
        else:
            sponsors.append({"name": name})
            index += 1
    return sponsors[:100]


def _parse_exhibitors(soup: BeautifulSoup) -> List[Dict[str, str]]:
    exhibitors: List[Dict[str, str]] = []
    for block in soup.select(".exhibitorsBlock"):
        name_node = block.select_one(".exhibitorName")
        name = _clean_text(name_node.get_text(" ", strip=True)) if name_node else None
        if not name or re.search(r"Talk And Connect|Ask your questions", name, re.I):
            continue
        text = _clean_text(block.get_text(" ", strip=True)) or ""
        item: Dict[str, str] = {
            "name": name,
            "relationshipType": "EXHIBITOR",
        }
        website = None
        for link in block.select("a[href]"):
            href = link.get("href") or ""
            link_text = _clean_text(link.get_text(" ", strip=True)) or ""
            if link_text.lower() == "website" or ("http" in href and "10times.com" not in href):
                website = href
                break
        if website:
            item["website"] = website
        edition = re.search(r"\bExhibited\s+In\s+(.+?)(?:\s+Follow|\s+Website|$)", text, re.I)
        if edition:
            item["edition"] = _clean_text(edition.group(1)) or edition.group(1)
        exhibitors.append(item)
    return _dedupe_named_items(exhibitors)[:500]


def _dedupe_named_items(items: List[Dict[str, str]]) -> List[Dict[str, str]]:
    deduped: List[Dict[str, str]] = []
    seen: set[str] = set()
    for item in items:
        key = (item.get("name") or "").strip().lower()
        if not key or key in seen:
            continue
        seen.add(key)
        deduped.append(item)
    return deduped


async def _auto_tick_cloudflare(page: Any) -> None:
    """Attempt to automatically tick the Cloudflare Turnstile verification checkbox."""
    try:
        iframes = page.frame_locator("iframe")
        count = await iframes.count()
        for i in range(count):
            iframe = iframes.nth(i)
            checkbox = iframe.locator("input[type='checkbox']")
            if await checkbox.count() > 0:
                await checkbox.first.click(timeout=2000)
                break
    except Exception as e:
        logger.debug(f"Error auto-ticking Cloudflare: {e}")


async def _wait_for_10times_otp(
    mail: TenMinuteMailClient,
    *,
    timeout_seconds: float,
    poll_interval_seconds: float,
) -> Optional[TemporaryEmailOtp]:
    """
    Wait for a 10times OTP using TenMinuteMailClient's messageCount-first polling.
    It only fetches message bodies with messagesAfter/<cached_count> after count increases.
    """
    return await mail.get_otp(
        timeout_seconds=timeout_seconds,
        poll_interval_seconds=poll_interval_seconds,
        otp_digits=4,
        sender_contains="10times",
    )


def _attach_event_api_listener(page: Any, event_api_calls: List[Dict[str, Any]]) -> None:
    async def on_response(response: Any) -> None:
        url = response.url
        if not _is_event_api_url(url):
            return

        request = response.request
        item: Dict[str, Any] = {
            "method": request.method,
            "url": url,
            "status": response.status,
            "requestPostData": request.post_data,
            "contentType": response.headers.get("content-type"),
        }
        try:
            body = await response.text()
            item["bodyPreview"] = body[:3000]
            parsed_json = None
            try:
                parsed_json = json.loads(body)
            except Exception:
                parsed_json = None
            if parsed_json is not None:
                item["json"] = parsed_json
        except Exception as error:
            item["bodyError"] = str(error)
        event_api_calls.append(item)

    page.on("response", lambda response: asyncio.create_task(on_response(response)))


def _is_event_api_url(url: str) -> bool:
    parsed = urlparse(url)
    host = parsed.netloc.lower()
    if not host.endswith("10times.com"):
        return False
    lowered = url.lower()
    if any(skip in lowered for skip in ("/cdn-cgi/", "google", "analytics", "/css/", "/images/")):
        return False
    return any(marker in lowered for marker in ("/ajax", "/api", "for=scroll", "conferences"))


async def _scroll_for_event_api(page: Any) -> None:
    for _ in range(4):
        await page.mouse.wheel(0, 1800)
        await page.wait_for_timeout(1500)


def _login_result(response: httpx.Response) -> TenTimesLoginResult:
    text = response.text
    return TenTimesLoginResult(
        ok=200 <= response.status_code < 400 and not _looks_blocked(text),
        status_code=response.status_code,
        url=str(response.url),
        message=_extract_login_message(text),
        cookies={cookie.name: cookie.value for cookie in response.cookies.jar},
        body_preview=_clean_text(text[:1200]) or "",
    )


def _extract_login_message(html: str) -> Optional[str]:
    text = _clean_text(re.sub(r"<[^>]+>", " ", html or ""))
    if not text:
        return None
    for marker in (
        "We have sent a 4 digit OTP",
        "Please enter the valid login code",
        "Nearby Explore Feed Login",
    ):
        index = text.find(marker)
        if index >= 0:
            return text[index : index + 240]
    return text[:240]


def _looks_blocked(text: str) -> bool:
    lowered = (text or "").lower()
    return "enable javascript and cookies" in lowered or "security verification" in lowered or _is_wall_html(text)


def _clean_text(value: Any) -> Optional[str]:
    if value is None:
        return None
    text = re.sub(r"\s+", " ", str(value)).strip()
    return text or None
