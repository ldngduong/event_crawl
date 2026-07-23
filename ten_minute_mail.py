from __future__ import annotations

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
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self._email: Optional[TemporaryEmail] = None
        self.preferred_domain_suffix = _normalize_domain_suffix(preferred_domain_suffix)
        self.max_address_attempts = max(1, max_address_attempts)
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
        return _to_int(response.json().get("messageCount"))

    async def get_mails(self, after_message_id: int | str = 0) -> List[Dict[str, Any]]:
        await self.get_gmail()
        response = await self._client.get(
            MESSAGES_AFTER_PATH.format(message_id=after_message_id)
        )
        response.raise_for_status()
        data = response.json()
        if not isinstance(data, list):
            raise ValueError(f"Unexpected messages response: {data!r}")
        return data

    async def get_otp(
        self,
        after_message_id: int | str = 0,
        timeout_seconds: float = 60.0,
        poll_interval_seconds: float = 2.0,
        otp_digits: int = 4,
        sender_contains: Optional[str] = "10times",
    ) -> Optional[TemporaryEmailOtp]:
        """
        Poll the current inbox and return the first matching OTP.

        For 10times emails, the OTP is usually present in both subject
        ("10times OTP - 9392") and body text.
        """
        deadline = time.monotonic() + timeout_seconds
        while True:
            messages = await self.get_mails(after_message_id=after_message_id)
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
            response.raise_for_status()
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
    poll_interval_seconds: float = 2.0,
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


def _html_to_text(value: str) -> str:
    without_tags = re.sub(r"<[^>]+>", " ", value)
    return unescape(re.sub(r"\s+", " ", without_tags))


async def asyncio_sleep(seconds: float) -> None:
    import asyncio

    await asyncio.sleep(seconds)
