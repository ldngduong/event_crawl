from __future__ import annotations

import json
import time
import re
from dataclasses import dataclass
from html import unescape
from typing import Any, Dict, List, Optional

import httpx


BASE_URL = "https://10minutemail.com"
SESSION_ADDRESS_PATH = "/session/address"
SECONDS_LEFT_PATH = "/session/secondsLeft"
MESSAGE_COUNT_PATH = "/messages/messageCount"
MESSAGES_AFTER_PATH = "/messages/messagesAfter/{message_id}"


@dataclass
class TemporaryEmail:
    address: str
    seconds_left: int
    expires_at: float


@dataclass
class TemporaryEmailOtp:
    otp: str
    message: Dict[str, Any]


@dataclass
class TemporaryEmailMessages:
    messages: List[Dict[str, Any]]
    message_count: int = 0
    rate_limited: bool = False
    retry_after_seconds: int = 0
    stale: bool = False


class TenMinuteMailClient:
    """
    Client for 10minutemail.com.

    The site binds the generated inbox to a JSESSIONID cookie. Keep one client
    instance alive and reuse it for every request that belongs to the same inbox.
    """

    def __init__(
        self,
        base_url: str = BASE_URL,
        timeout: float = 30.0,
        headers: Optional[Dict[str, str]] = None,
        preferred_domain_suffix: Optional[str] = ".net",
        max_address_attempts: int = 10,
        messages_cache_seconds: float = 30.0,
        browser_bootstrap_on_forbidden: bool = True,
        browser_headless: bool = False,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self._email: Optional[TemporaryEmail] = None
        self._messages_cache: Dict[str, List[Dict[str, Any]]] = {}
        self._messages_cache_at: Dict[str, float] = {}
        self._last_message_count: Optional[int] = None
        self._rate_limited_until = 0.0
        self.messages_cache_seconds = max(0.0, messages_cache_seconds)
        self.preferred_domain_suffix = _normalize_domain_suffix(preferred_domain_suffix)
        self.max_address_attempts = max(1, max_address_attempts)
        self.browser_bootstrap_on_forbidden = browser_bootstrap_on_forbidden
        self.browser_headless = browser_headless
        self._client = httpx.AsyncClient(
            base_url=self.base_url,
            follow_redirects=True,
            timeout=timeout,
            headers={
                "Accept": "application/json, text/plain, */*",
                "User-Agent": (
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/126.0.0.0 Safari/537.36"
                ),
                "Referer": f"{self.base_url}/",
                **(headers or {}),
            },
        )

    async def __aenter__(self) -> "TenMinuteMailClient":
        return self

    async def __aexit__(self, *_: object) -> None:
        await self.aclose()

    async def aclose(self) -> None:
        await self._client.aclose()

    def set_preferred_domain_suffix(self, value: Optional[str]) -> None:
        self.preferred_domain_suffix = _normalize_domain_suffix(value)

    async def get_gmail(self, force_new: bool = False) -> TemporaryEmail:
        """
        Return the current temporary email and its remaining lifetime.

        A new email is created automatically when there is no cached inbox or
        the previous inbox has expired.
        """
        if force_new or self._email is None or self._email.seconds_left <= 0:
            self._email = await self._create_email()
            return self._email

        seconds_left = await self.get_seconds_left()
        if seconds_left <= 0:
            self._email = await self._create_email()
            return self._email

        self._email = TemporaryEmail(
            address=self._email.address,
            seconds_left=seconds_left,
            expires_at=time.time() + seconds_left,
        )
        return self._email

    async def get_seconds_left(self) -> int:
        response = await self._client.get(SECONDS_LEFT_PATH)
        response.raise_for_status()
        return _to_int(response.json().get("secondsLeft"))

    async def get_message_count(self) -> int:
        await self.get_gmail()
        response = await self._client.get(MESSAGE_COUNT_PATH)
        response.raise_for_status()
        message_count = _to_int(response.json().get("messageCount"))
        self._last_message_count = message_count
        return message_count

    async def get_mails(self, after_message_id: int | str = 0) -> List[Dict[str, Any]]:
        await self.get_gmail()
        result = await self.get_messages_result(after_message_id=after_message_id)
        if result.rate_limited and not result.stale:
            raise TenMinuteMailRateLimitError(result.retry_after_seconds)
        return result.messages

    async def get_messages_result(self, after_message_id: int | str = 0) -> TemporaryEmailMessages:
        await self.get_gmail()
        explicit_after = str(after_message_id) not in ("", "0")
        cache_key = str(after_message_id) if explicit_after else "all"
        now = time.monotonic()
        cached = self._messages_cache.get(cache_key)
        cache_age = now - self._messages_cache_at.get(cache_key, 0.0)
        if cached is not None and cache_age <= self.messages_cache_seconds:
            return TemporaryEmailMessages(
                messages=cached,
                message_count=self._last_message_count if self._last_message_count is not None else len(cached),
            )
        if cached is not None and now < self._rate_limited_until:
            return TemporaryEmailMessages(
                messages=cached,
                message_count=self._last_message_count if self._last_message_count is not None else len(cached),
                rate_limited=True,
                retry_after_seconds=self.rate_limited_seconds_left(),
                stale=True,
            )

        message_count = await self.get_message_count()
        cached_count = len(cached or [])
        if message_count <= 0:
            self._messages_cache[cache_key] = []
            self._messages_cache_at[cache_key] = time.monotonic()
            return TemporaryEmailMessages(messages=[], message_count=message_count)
        if cached is not None and message_count <= cached_count:
            self._messages_cache_at[cache_key] = time.monotonic()
            return TemporaryEmailMessages(messages=cached, message_count=message_count)

        messages_after = str(after_message_id) if explicit_after else str(cached_count)

        response = await self._client.get(
            MESSAGES_AFTER_PATH.format(message_id=messages_after)
        )
        try:
            response.raise_for_status()
        except httpx.HTTPStatusError as error:
            if error.response.status_code == 429:
                self._rate_limited_until = time.monotonic() + _retry_after_seconds(
                    error.response,
                    default_seconds=max(60.0, self.messages_cache_seconds * 2),
                )
            if cached is not None and error.response.status_code == 429:
                return TemporaryEmailMessages(
                    messages=cached,
                    message_count=message_count,
                    rate_limited=True,
                    retry_after_seconds=self.rate_limited_seconds_left(),
                    stale=True,
                )
            if error.response.status_code == 429:
                return TemporaryEmailMessages(
                    messages=[],
                    message_count=message_count,
                    rate_limited=True,
                    retry_after_seconds=self.rate_limited_seconds_left(),
                    stale=False,
                )
            raise

        data = response.json()
        if not isinstance(data, list):
            raise ValueError(f"Unexpected messages response: {data!r}")
        messages = _merge_messages(cached or [], data)
        self._messages_cache[cache_key] = messages
        self._messages_cache_at[cache_key] = time.monotonic()
        self._rate_limited_until = 0.0
        return TemporaryEmailMessages(messages=messages, message_count=message_count)

    def rate_limited_seconds_left(self) -> int:
        return max(0, int(self._rate_limited_until - time.monotonic()))

    async def get_otp(
        self,
        after_message_id: int | str = 0,
        timeout_seconds: float = 60.0,
        poll_interval_seconds: float = 5.0,
        otp_digits: int = 4,
        sender_contains: Optional[str] = "10times",
    ) -> Optional[TemporaryEmailOtp]:
        """
        Poll the current inbox and return the first matching OTP.

        For 10times emails, the OTP is usually present in both subject
        ("10times OTP - 9392") and body text.
        """
        deadline = time.monotonic() + timeout_seconds
        rate_limit_delay_seconds = max(10.0, poll_interval_seconds * 3)
        while True:
            try:
                messages = await self.get_mails(after_message_id=after_message_id)
            except httpx.HTTPStatusError as error:
                if error.response.status_code != 429:
                    raise
                if time.monotonic() >= deadline:
                    return None
                await asyncio_sleep(rate_limit_delay_seconds)
                continue

            otp = extract_otp_from_messages(
                messages,
                otp_digits=otp_digits,
                sender_contains=sender_contains,
            )
            if otp is not None:
                return otp

            if time.monotonic() >= deadline:
                return None
            await asyncio_sleep(poll_interval_seconds)

    async def _create_email(self) -> TemporaryEmail:
        last_address: Optional[str] = None
        for _ in range(self.max_address_attempts):
            response = await self._client.get(SESSION_ADDRESS_PATH)
            try:
                response.raise_for_status()
            except httpx.HTTPStatusError as error:
                if error.response.status_code == 403 and self.browser_bootstrap_on_forbidden:
                    return await self._create_email_with_browser()
                raise
            address = response.json().get("address")
            if not isinstance(address, str) or not address:
                raise ValueError(f"Unexpected address response: {response.text}")
            last_address = address
            if self._accepts_address(address):
                break

            await self.aclose()
            self._client = self._new_client()
        else:
            raise ValueError(
                "Could not get a temporary email matching "
                f"{self.preferred_domain_suffix!r}; last address was {last_address!r}"
            )

        seconds_left = await self.get_seconds_left()
        return self._finalize_email(address, seconds_left)

    async def _create_email_with_browser(self) -> TemporaryEmail:
        from playwright.async_api import async_playwright

        last_address: Optional[str] = None
        last_browser_error: Optional[Dict[str, Any]] = None
        async with async_playwright() as p:
            browser = await p.chromium.launch(
                headless=self.browser_headless,
                args=["--disable-blink-features=AutomationControlled"],
            )
            try:
                for _ in range(self.max_address_attempts):
                    context = await browser.new_context(viewport={"width": 1365, "height": 900})
                    page = await context.new_page()
                    await page.goto(f"{self.base_url}/", wait_until="domcontentloaded", timeout=90000)
                    await page.wait_for_timeout(5000)

                    address_payload = await page.evaluate(
                        """
                        async () => {
                            const response = await fetch('/session/address', {
                                credentials: 'include',
                                headers: { Accept: 'application/json, text/plain, */*' }
                            });
                            return { status: response.status, text: await response.text() };
                        }
                        """
                    )
                    if int(address_payload.get("status") or 0) >= 400:
                        last_browser_error = {
                            "status": address_payload.get("status"),
                            "text": str(address_payload.get("text") or "")[:500],
                        }
                        await context.close()
                        continue

                    address_data = json.loads(address_payload.get("text") or "{}")
                    address = address_data.get("address")
                    if not isinstance(address, str) or not address:
                        await context.close()
                        raise ValueError(f"Unexpected browser address response: {address_payload!r}")
                    last_address = address

                    seconds_payload = await page.evaluate(
                        """
                        async () => {
                            const response = await fetch('/session/secondsLeft', {
                                credentials: 'include',
                                headers: { Accept: 'application/json, text/plain, */*' }
                            });
                            return { status: response.status, text: await response.text() };
                        }
                        """
                    )
                    if int(seconds_payload.get("status") or 0) >= 400:
                        last_browser_error = {
                            "status": seconds_payload.get("status"),
                            "text": str(seconds_payload.get("text") or "")[:500],
                        }
                        await context.close()
                        continue

                    cookies = await context.cookies(self.base_url)
                    for cookie in cookies:
                        self._client.cookies.set(
                            cookie["name"],
                            cookie["value"],
                            domain=cookie.get("domain") or "10minutemail.com",
                            path=cookie.get("path") or "/",
                        )
                    await context.close()

                    if self._accepts_address(address):
                        seconds_data = json.loads(seconds_payload.get("text") or "{}")
                        return self._finalize_email(
                            address,
                            _to_int(seconds_data.get("secondsLeft")),
                        )
            finally:
                await browser.close()

        raise ValueError(
            "Could not get a temporary email matching "
            f"{self.preferred_domain_suffix!r} with browser bootstrap; "
            f"last address was {last_address!r}; last browser error was {last_browser_error!r}"
        )

    def _finalize_email(self, address: str, seconds_left: int) -> TemporaryEmail:
        self._messages_cache.clear()
        self._messages_cache_at.clear()
        self._last_message_count = None
        self._rate_limited_until = 0.0
        return TemporaryEmail(
            address=address,
            seconds_left=seconds_left,
            expires_at=time.time() + seconds_left,
        )

    def _new_client(self) -> httpx.AsyncClient:
        return httpx.AsyncClient(
            base_url=self.base_url,
            follow_redirects=True,
            timeout=self._client.timeout,
            headers=self._client.headers,
        )

    def _accepts_address(self, address: str) -> bool:
        if self.preferred_domain_suffix is None:
            return True
        return address.lower().endswith(self.preferred_domain_suffix)


async def get_gmail(client: Optional[TenMinuteMailClient] = None) -> TemporaryEmail:
    if client is not None:
        return await client.get_gmail()

    async with TenMinuteMailClient() as temporary_mail:
        return await temporary_mail.get_gmail()


async def get_mails(
    client: TenMinuteMailClient,
    after_message_id: int | str = 0,
) -> List[Dict[str, Any]]:
    return await client.get_mails(after_message_id=after_message_id)


async def get_otp(
    client: TenMinuteMailClient,
    after_message_id: int | str = 0,
    timeout_seconds: float = 60.0,
    poll_interval_seconds: float = 5.0,
    otp_digits: int = 4,
    sender_contains: Optional[str] = "10times",
) -> Optional[TemporaryEmailOtp]:
    return await client.get_otp(
        after_message_id=after_message_id,
        timeout_seconds=timeout_seconds,
        poll_interval_seconds=poll_interval_seconds,
        otp_digits=otp_digits,
        sender_contains=sender_contains,
    )


def extract_otp_from_messages(
    messages: List[Dict[str, Any]],
    otp_digits: int = 4,
    sender_contains: Optional[str] = "10times",
) -> Optional[TemporaryEmailOtp]:
    for message in sorted(messages, key=_message_sent_date, reverse=True):
        if sender_contains and not _message_contains(message, sender_contains):
            continue

        otp = extract_otp_from_message(message, otp_digits=otp_digits)
        if otp:
            return TemporaryEmailOtp(otp=otp, message=message)
    return None


def extract_otp_from_message(
    message: Dict[str, Any],
    otp_digits: int = 4,
) -> Optional[str]:
    pattern = re.compile(rf"(?<!\d)(\d{{{otp_digits}}})(?!\d)")
    text_parts = [
        str(message.get("subject") or ""),
        str(message.get("bodyPreview") or ""),
        str(message.get("bodyPlainText") or ""),
        _html_to_text(str(message.get("bodyHtmlContent") or "")),
    ]
    joined_text = "\n".join(text_parts)

    otp_context = re.search(
        rf"otp[^\d]{{0,40}}(\d{{{otp_digits}}})",
        joined_text,
        flags=re.IGNORECASE,
    )
    if otp_context:
        return otp_context.group(1)

    match = pattern.search(joined_text)
    if match:
        return match.group(1)
    return None


def _to_int(value: Any) -> int:
    try:
        return int(value)
    except (TypeError, ValueError) as error:
        raise ValueError(f"Expected integer-like value, got {value!r}") from error


def _retry_after_seconds(response: httpx.Response, default_seconds: float) -> float:
    retry_after = response.headers.get("Retry-After")
    if retry_after:
        try:
            return max(1.0, float(retry_after))
        except ValueError:
            return default_seconds
    return default_seconds


class TenMinuteMailRateLimitError(Exception):
    def __init__(self, retry_after_seconds: int) -> None:
        self.retry_after_seconds = retry_after_seconds
        super().__init__(
            f"10minutemail is rate limited; retry after {retry_after_seconds} seconds"
        )


def _normalize_domain_suffix(value: Optional[str]) -> Optional[str]:
    if value is None:
        return None

    suffix = value.strip().lower()
    if not suffix:
        return None
    if "@" in suffix:
        suffix = suffix.rsplit("@", 1)[1]
    if not suffix.startswith("."):
        suffix = f".{suffix}"
    return suffix


def _message_sent_date(message: Dict[str, Any]) -> str:
    return str(message.get("sentDate") or "")


def _message_contains(message: Dict[str, Any], needle: str) -> bool:
    haystack = " ".join(
        str(message.get(key) or "")
        for key in ("sender", "from", "subject", "bodyPreview", "bodyPlainText")
    )
    return needle.lower() in haystack.lower()


def _merge_messages(
    current: List[Dict[str, Any]],
    incoming: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    merged: List[Dict[str, Any]] = []
    seen: set[str] = set()
    for message in [*current, *incoming]:
        key = str(message.get("id") or message.get("sentDate") or message.get("subject") or len(merged))
        if key in seen:
            continue
        seen.add(key)
        merged.append(message)
    return merged


def _html_to_text(value: str) -> str:
    without_tags = re.sub(r"<[^>]+>", " ", value)
    return unescape(re.sub(r"\s+", " ", without_tags))


async def asyncio_sleep(seconds: float) -> None:
    import asyncio

    await asyncio.sleep(seconds)
