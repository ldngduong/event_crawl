import os
import httpx
from typing import List, Dict, Any, Optional

DEFAULT_EAGLE_API_BASE_URL = "http://localhost:3001/api/v1"
DEFAULT_EAGLE_IMPORT_BATCH_SIZE = 50

def map_to_generic_event(raw_event: Dict[str, Any], source_provider: str) -> Dict[str, Any]:
    """
    Map an event from a specific scraper source to the generic backend format.
    """
    
    name = raw_event.get("name") or raw_event.get("title") or "Unknown"
    
    source_url = raw_event.get("url") or raw_event.get("source_url") or raw_event.get("sourceUrl")
    
    start_at = raw_event.get("startDate") or raw_event.get("start_at") or raw_event.get("start")
    end_at = raw_event.get("endDate") or raw_event.get("end_at") or raw_event.get("end")
    
    description = raw_event.get("description")
    
    organizer = raw_event.get("organizer")
    organizer_name = organizer.get("name") if isinstance(organizer, dict) else None
    
    location = raw_event.get("location")
    if not isinstance(location, dict):
        location = {}
        
    address = location.get("address")
    if not isinstance(address, dict):
        address = {}
        
    city = address.get("addressLocality") or raw_event.get("city")
    country = address.get("addressCountry") or raw_event.get("country")
    
    occurrence = {
        "locationText": location.get("name") or raw_event.get("locationText"),
        "latitude": raw_event.get("lat") or raw_event.get("latitude") or location.get("latitude"),
        "longitude": raw_event.get("lng") or raw_event.get("longitude") or location.get("longitude"),
        "venueName": location.get("name"),
        "streetAddress": address.get("streetAddress"),
        "city": city,
        "region": address.get("addressRegion"),
        "postalCode": address.get("postalCode"),
        "country": country,
        "timezone": raw_event.get("timezone"),
        "expectedAttendance": raw_event.get("expectedAttendance")
    }
    
    occurrence = {k: v for k, v in occurrence.items() if v is not None}
    
    return {
        "name": name,
        "sourceUrl": source_url,
        "startAt": start_at,
        "endAt": end_at,
        "city": city,
        "country": country,
        "eventType": raw_event.get("eventType") or raw_event.get("@type"),
        "organizerName": organizer_name,
        "description": description,
        "sourceProvider": source_provider,
        "occurrence": occurrence,
        "metadataJson": raw_event
    }

async def ingest_generic_events_to_eagle(
    *,
    organization_id: Optional[str],
    workspace_id: Optional[str],
    events: List[Dict[str, Any]],
    source_provider: str,
    parse_failures: Optional[List[Dict[str, Any]]] = None,
    persist: bool = False,
    already_mapped: bool = False,
) -> Dict[str, Any]:
    
    if already_mapped:
        mapped_events = events
    else:
        mapped_events = [map_to_generic_event(e, source_provider) for e in events]
    
    eagle_api_base_url = os.getenv("EAGLE_API_BASE_URL", DEFAULT_EAGLE_API_BASE_URL).rstrip("/")
    endpoint_url = f"{eagle_api_base_url}/scraper/events/generic-import"
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
            "events": mapped_events,
            "results": [],
            "failures": [],
            "parse_failures": parse_failures or [],
        }

    async with httpx.AsyncClient(timeout=120) as client:
        for start in range(0, len(mapped_events), batch_size):
            batch = mapped_events[start : start + batch_size]
            payload = {
                "events": batch,
            }
            if organization_id:
                payload["organizationId"] = organization_id
            if workspace_id:
                payload["workspaceId"] = workspace_id

            batch_meta = {
                "batch_start": start,
                "batch_end": start + len(batch) - 1,
                "event_count": len(batch),
            }

            try:
                response = await client.post(endpoint_url, json=payload)
                response.raise_for_status()
                raw_response = response.json()
                # Backend wraps response in {status_code, message, error, data: {...}}
                eagle_response = raw_response.get("data") or raw_response
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

    imported_count = sum(r.get("eagle_response", {}).get("count") or 0 for r in results)
    created_count = sum(r.get("eagle_response", {}).get("created") or 0 for r in results)
    updated_count = sum(r.get("eagle_response", {}).get("updated") or 0 for r in results)
    skipped_count = sum(r.get("eagle_response", {}).get("skipped") or 0 for r in results)

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
        "events": mapped_events,
        "results": results,
        "failures": failures,
        "parse_failures": parse_failures or [],
    }
