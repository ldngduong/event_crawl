import logging
import os
from typing import Any, Dict, Optional, Tuple

import httpx

logger = logging.getLogger(__name__)

OPENCAGE_GEOCODE_URL = "https://api.opencagedata.com/geocode/v1/json"
_TIMEOUT_SECONDS = 20


def _first_number(*values: Any) -> Optional[float]:
    for v in values:
        if v is None:
            continue
        try:
            n = float(v)
            if n == n:
                return n
        except (TypeError, ValueError):
            continue
    return None


def _get_api_key() -> Optional[str]:
    return os.getenv("OPENCAGE_API_KEY")


# ---------------------------------------------------------------------------
# Text -> Coordinates
# ---------------------------------------------------------------------------

async def text_to_coords(
    query: str,
    *,
    client: Optional[httpx.AsyncClient] = None,
) -> Optional[Tuple[float, float]]:
    if not query or not query.strip():
        return None

    api_key = _get_api_key()
    if not api_key:
        logger.warning("OPENCAGE_API_KEY not set")
        return None

    async def _call(c: httpx.AsyncClient) -> Optional[Tuple[float, float]]:
        response = await c.get(
            OPENCAGE_GEOCODE_URL,
            params={"q": query, "key": api_key, "limit": "1", "no_annotations": "1"},
            timeout=_TIMEOUT_SECONDS,
        )
        response.raise_for_status()
        payload = response.json()
        result = (payload.get("results") or [{}])[0]
        geometry = result.get("geometry") if isinstance(result, dict) else {}
        lat = _first_number(geometry.get("lat") if isinstance(geometry, dict) else None)
        lng = _first_number(geometry.get("lng") if isinstance(geometry, dict) else None)
        if lat is not None and lng is not None:
            return lat, lng
        return None

    if client is not None:
        return await _call(client)

    async with httpx.AsyncClient() as new_client:
        return await _call(new_client)


# ---------------------------------------------------------------------------
# Coordinates -> Text (reverse geocoding)
# ---------------------------------------------------------------------------

async def coords_to_text(
    lat: float,
    lng: float,
    *,
    client: Optional[httpx.AsyncClient] = None,
) -> Optional[Dict[str, Any]]:
    """Convert ``(latitude, longitude)`` to a reverse-geocoded address dict via OpenCage.

    Returns dict with keys: ``formatted``, ``city``, ``state``, ``country``,
    ``postcode``, ``road`` — or ``None``.
    """
    if not (-90 <= lat <= 90) or not (-180 <= lng <= 180):
        logger.warning("Invalid coordinates: lat=%s lng=%s", lat, lng)
        return None

    api_key = _get_api_key()
    if not api_key:
        logger.warning("OPENCAGE_API_KEY not set")
        return None

    async def _call(c: httpx.AsyncClient) -> Optional[Dict[str, Any]]:
        response = await c.get(
            OPENCAGE_GEOCODE_URL,
            params={"q": f"{lat},{lng}", "key": api_key, "limit": "1", "no_annotations": "1"},
            timeout=_TIMEOUT_SECONDS,
        )
        response.raise_for_status()
        payload = response.json()
        result = (payload.get("results") or [{}])[0]
        if not result:
            return None
        components = result.get("components") if isinstance(result, dict) else {}
        return {
            "formatted": result.get("formatted", ""),
            "city": components.get("city"),
            "state": components.get("state"),
            "country": components.get("country"),
            "postcode": components.get("postcode"),
            "road": components.get("road"),
        }

    if client is not None:
        return await _call(client)

    async with httpx.AsyncClient() as new_client:
        return await _call(new_client)
