import httpx
from bs4 import BeautifulSoup
import logging
import asyncio
from typing import List, Dict, Any, Optional
import json

from generic_mapper import ingest_generic_events_to_eagle
from schemas import GenericAttendeeDict, GenericCompanyDict, GenericMappedEventDict, GenericOccurrenceDict

# Tắt log INFO mặc định của httpx để đỡ rác màn hình
logging.getLogger("httpx").setLevel(logging.WARNING)
logger = logging.getLogger(__name__)

def parse_conferank_date(date_str: str) -> tuple[str, str]:
    # Ví dụ date_str: "Jul 19, 2026 - Jul 23, 2026" hoặc "Jul 19, 2026"
    parts = date_str.split(' - ')
    start_date = parts[0].strip() if len(parts) > 0 else ""
    end_date = parts[1].strip() if len(parts) > 1 else start_date
    return start_date, end_date

def parse_conferank_location(loc_str: str) -> tuple[str, str]:
    # Ví dụ loc_str: "Los Angeles, United States"
    parts = loc_str.split(', ')
    city = parts[0].strip() if len(parts) > 0 else ""
    country = parts[1].strip() if len(parts) > 1 else ""
    return city, country

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

def _is_placeholder_speaker_name(name: Optional[str]) -> bool:
    if not name:
        return True
    normalized = name.strip().lower()
    return normalized in {
        "speaker",
        "speakers",
        "speakers coming soon",
        "coming soon",
        "to be announced",
        "tba",
        "tbd",
    }

def _has_speaker_details(speaker: Dict[str, Any]) -> bool:
    name = _first_string(speaker.get("name"))
    if _is_placeholder_speaker_name(name):
        return False
    return bool(_first_string(speaker.get("role"), speaker.get("linkedin")))

async def enrich_event_details(client: httpx.AsyncClient, event_url: str) -> Dict[str, Any]:
    enriched_data = {}
    
    async def fetch_overview():
        try:
            overview_resp = await client.get(event_url)
            if overview_resp.status_code == 200:
                overview_soup = BeautifulSoup(overview_resp.text, 'html.parser')
                prose_div = overview_soup.find('div', class_='prose')
                if prose_div:
                    enriched_data['full_description'] = prose_div.text.strip()
                extra_info = []
                for h3 in overview_soup.find_all('h3'):
                    title = h3.text.strip()
                    if title in ["Conference Highlights", "Target Audience"]:
                        content_div = h3.parent
                        if content_div:
                            content_text = content_div.get_text(separator='\n', strip=True).replace(title, '').strip()
                            extra_info.append(f"**{title}**\n{content_text}")
                if extra_info:
                    enriched_data['extra_description'] = "\n\n".join(extra_info)
        except Exception as e:
            logger.warning(f"Failed to fetch overview for {event_url}: {e}")
            
    async def fetch_sponsors():
        try:
            sponsors_resp = await client.get(f"{event_url}/sponsors")
            if sponsors_resp.status_code == 200:
                sponsors_soup = BeautifulSoup(sponsors_resp.text, 'html.parser')
                sponsors_list = []
                for h3 in sponsors_soup.find_all('h3'):
                    h3_text = h3.text.strip().upper()
                    if "SPONSOR" in h3_text:
                        tier = h3_text.replace("SPONSORS", "").replace("SPONSOR", "").strip()
                        tier_display = f"{tier} Sponsor" if tier else "Sponsor"
                        tier_display = tier_display.title()
                        container = h3.find_next_sibling('div')
                        if container:
                            cards = container.find_all('div', class_=lambda c: c and 'luxury-card' in c)
                            if not cards: 
                                cards = [a.parent.parent for a in container.find_all('a', href=lambda href: href and '/sponsors/' in href) if a.parent and a.parent.parent]
                            for card in cards:
                                h4 = card.find('h4')
                                if h4:
                                    name = h4.text.strip()
                                    if name:
                                        desc_p = card.find('p', class_=lambda c: c and 'text-gray-600' in c)
                                        desc = desc_p.text.strip() if desc_p else ""
                                        website_a = card.find('a', string=lambda s: s and "Website" in s)
                                        if not website_a:
                                            website_a = card.find('a', title="Website")
                                        website = website_a['href'] if website_a else ""
                                        info = {"name": name, "tier": tier_display, "description": desc, "website": website}
                                        if info not in sponsors_list:
                                            sponsors_list.append(info)
                if sponsors_list:
                    enriched_data['sponsors'] = sponsors_list
        except Exception as e:
            logger.warning(f"Failed to fetch sponsors for {event_url}: {e}")
            
    async def fetch_venue():
        try:
            venue_resp = await client.get(f"{event_url}/venue")
            if venue_resp.status_code == 200:
                venue_soup = BeautifulSoup(venue_resp.text, 'html.parser')
                map_container = venue_soup.find(id='map-container')
                if map_container:
                    if map_container.get('data-venue-name'):
                        enriched_data['venue_name'] = map_container.get('data-venue-name')
                    if map_container.get('data-venue-address'):
                        enriched_data['venue_address'] = map_container.get('data-venue-address')
                    if map_container.get('data-venue-latitude'):
                        enriched_data['latitude'] = map_container.get('data-venue-latitude')
                    if map_container.get('data-venue-longitude'):
                        enriched_data['longitude'] = map_container.get('data-venue-longitude')
                else:
                    h1 = venue_soup.find('h1')
                    if h1 and h1.find_next_sibling('p'):
                        p_tag = h1.find_next_sibling('p')
                        strong_tag = p_tag.find('strong')
                        if strong_tag:
                            enriched_data['venue_name'] = strong_tag.text.strip()
                for h3 in venue_soup.find_all('h3'):
                    if h3.text.strip() == "Venue Features":
                        feature_spans = h3.parent.find_all('span', class_='text-gray-700')
                        if feature_spans:
                            enriched_data['venue_features'] = [span.text.strip() for span in feature_spans if span.text.strip()]
        except Exception as e:
            logger.warning(f"Failed to fetch venue for {event_url}: {e}")
            
    async def fetch_speakers():
        try:
            speakers_resp = await client.get(f"{event_url}/speakers")
            if speakers_resp.status_code == 200:
                speakers_soup = BeautifulSoup(speakers_resp.text, 'html.parser')
                speakers = []
                for h3 in speakers_soup.find_all('h3'):
                    name = h3.text.strip()
                    if name.lower() != 'conferank' and name:
                        speaker_card = h3.find_parent('div', class_=lambda c: c and ('bg-white' in c or 'p-6' in c))
                        if not speaker_card:
                            continue
                        role = ""
                        role_tag = speaker_card.find('p', class_=lambda c: c and ('text-blue-600' in c or 'text-gray-800' in c))
                        if role_tag:
                            role = role_tag.text.strip()
                            company_tag = role_tag.find_next_sibling('p', class_=lambda c: c and 'text-gray-600' in c)
                            if company_tag:
                                company_text = company_tag.text.strip()
                                if company_text:
                                    role += f" @ {company_text}"
                        linkedin_url = ""
                        linkedin_a = speaker_card.find('a', href=lambda href: href and 'linkedin.com' in href.lower())
                        if linkedin_a:
                            linkedin_url = linkedin_a['href']
                        speaker_obj = {"name": name, "role": role, "linkedin": linkedin_url}
                        if _has_speaker_details(speaker_obj) and speaker_obj not in speakers:
                            speakers.append(speaker_obj)
                if speakers:
                    enriched_data['speakers'] = speakers
        except Exception as e:
            logger.warning(f"Failed to fetch speakers for {event_url}: {e}")

    await asyncio.gather(fetch_overview(), fetch_sponsors(), fetch_venue(), fetch_speakers())
    return enriched_data

async def crawl_conferank_events(
    limit: int = 100, 
    enrich_details: bool = False,
    location: Optional[str] = None,
    date_from: Optional[str] = None,
    date_to: Optional[str] = None
) -> List[Dict[str, Any]]:
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36"
    }
    
    events_data = []
    
    async with httpx.AsyncClient(headers=headers, follow_redirects=True) as client:
        logger.info("Đang quét danh sách sự kiện từ Conferank...")
        
        page = 1
        seen_urls = set()
        
        url = "https://www.conferank.com/conferences"
        logger.info(f"Đang fetch trang chủ sự kiện: {url}")
        response = await client.get(url)
        soup = BeautifulSoup(response.text, 'html.parser')
        
        cards = soup.find_all('div', class_='conference-content')
        
        if cards:
            for card in cards:
                if len(events_data) >= limit:
                    break
                    
                title_tag = card.find('h3', class_='conference-title')
                if not title_tag or not title_tag.find('a'):
                    continue
                    
                a_tag = title_tag.find('a')
                title = a_tag.text.strip()
                url_path = a_tag['href']
                event_url = f"https://www.conferank.com{url_path}" if url_path.startswith('/') else url_path
                
                if event_url in seen_urls:
                    continue
                seen_urls.add(event_url)
                
                desc_tag = card.find('p', class_='conference-description')
                description = desc_tag.text.strip() if desc_tag else ""
                
                detail_items = card.find_all('div', class_='conference-detail-item')
                date_str = detail_items[0].text.strip() if len(detail_items) > 0 else ""
                location_str = detail_items[1].text.strip() if len(detail_items) > 1 else ""
                
                price_div = card.find('div', class_='conference-price')
                price = price_div.text.strip() if price_div else ""
                
                tags = [tag.text.strip() for tag in card.find_all('a', class_='conference-tag')]
                
                start_date, end_date = parse_conferank_date(date_str)
                
                # Filter by date_from / date_to
                from datetime import datetime
                if start_date and (date_from or date_to):
                    try:
                        sd = datetime.strptime(start_date, "%b %d, %Y")
                        if date_from:
                            df = datetime.strptime(date_from, "%Y-%m-%d")
                            if sd < df:
                                continue
                        if date_to:
                            dt = datetime.strptime(date_to, "%Y-%m-%d")
                            if sd > dt:
                                continue
                    except ValueError:
                        pass
                
                city, country = parse_conferank_location(location_str)
                
                # Filter by location (in-memory)
                if location and location.lower() not in location_str.lower():
                    continue
                
                event_payload = {
                    "@type": "Event",
                    "name": title,
                    "url": event_url,
                    "description": description,
                    "startDate": start_date,
                    "endDate": end_date,
                    "offers": {
                        "@type": "Offer",
                        "price": price
                    } if price else None,
                    "keywords": tags,
                    "location": {
                        "@type": "Place",
                        "address": {
                            "@type": "PostalAddress",
                            "addressLocality": city,
                            "addressCountry": country
                        }
                    }
                }
                events_data.append(event_payload)
            
        if enrich_details:
            logger.info(f"Đang cào chi tiết cho {len(events_data)} sự kiện một cách song song...")
            sem = asyncio.Semaphore(5) # Giới hạn 5 sự kiện cùng lúc để tránh bị block
            
            async def process_event(ev: Dict[str, Any]):
                async with sem:
                    enriched = await enrich_event_details(client, ev["url"])
                    if enriched:
                        ev["enriched_details"] = enriched
                        if "venue_name" in enriched:
                            ev["location"]["name"] = enriched["venue_name"]
                        if "venue_address" in enriched:
                            ev["location"]["address"]["streetAddress"] = enriched["venue_address"]
                        if "latitude" in enriched:
                            ev["location"]["latitude"] = float(enriched["latitude"])
                        if "longitude" in enriched:
                            ev["location"]["longitude"] = float(enriched["longitude"])
                            
                        desc_parts = []
                        if ev.get("description"):
                            desc_parts.append(ev["description"])
                        if "full_description" in enriched and enriched["full_description"] != ev.get("description"):
                            desc_parts.append(enriched["full_description"])
                        if "extra_description" in enriched:
                            desc_parts.append(enriched["extra_description"])
                        if "venue_features" in enriched:
                            features_str = "**Venue Features**\n" + "\n".join([f"• {f}" for f in enriched["venue_features"]])
                            desc_parts.append(features_str)
                        if "speakers" in enriched:
                            speaker_lines = []
                            for s in enriched["speakers"]:
                                if not _has_speaker_details(s):
                                    continue
                                line = f"• {s['name']}"
                                if s['role']: line += f" ({s['role']})"
                                if s['linkedin']: line += f" [LinkedIn: {s['linkedin']}]"
                                speaker_lines.append(line)
                            if speaker_lines:
                                speakers_str = "**Speakers**\n" + "\n".join(speaker_lines)
                                desc_parts.append(speakers_str)
                        if "sponsors" in enriched:
                            sponsor_lines = []
                            for s in enriched["sponsors"]:
                                line = f"• {s['name']} ({s['tier']})"
                                if s['website']: line += f" [Website: {s['website']}]"
                                if s['description']: line += f" - {s['description']}"
                                sponsor_lines.append(line)
                            sponsors_str = "**Sponsors**\n" + "\n".join(sponsor_lines)
                            desc_parts.append(sponsors_str)
                            
                        if desc_parts:
                            ev["description"] = "\n\n".join(desc_parts)
                            
                        enriched.pop("full_description", None)
                        enriched.pop("extra_description", None)
                        
            # Chạy đồng thời tất cả các event
            await asyncio.gather(*(process_event(ev) for ev in events_data))
            
    logger.info(f"Đã cào thành công {len(events_data)} sự kiện từ danh sách!")
    return [mapped for event in events_data if (mapped := _map_conferank_event_to_generic(event))]

def _map_conferank_event_to_generic(raw_event: Dict[str, Any]) -> Optional[GenericMappedEventDict]:
    name = _first_string(raw_event.get("name"), raw_event.get("title"))
    if not name:
        return None

    location = raw_event.get("location") if isinstance(raw_event.get("location"), dict) else {}
    address = location.get("address") if isinstance(location.get("address"), dict) else {}
    offers = raw_event.get("offers") if isinstance(raw_event.get("offers"), dict) else {}
    enriched = raw_event.get("enriched_details") if isinstance(raw_event.get("enriched_details"), dict) else {}
    categories = raw_event.get("keywords") if isinstance(raw_event.get("keywords"), list) else []

    venue_name = _first_string(location.get("name"), enriched.get("venue_name"))
    street_address = _first_string(address.get("streetAddress"), enriched.get("venue_address"))
    city = _first_string(address.get("addressLocality"))
    country = _first_string(address.get("addressCountry"))

    speakers = enriched.get("speakers") if isinstance(enriched.get("speakers"), list) else []
    attendees: List[GenericAttendeeDict] = []
    for speaker in speakers:
        if not isinstance(speaker, dict):
            continue
        speaker_name = _first_string(speaker.get("name"))
        if not speaker_name or not _has_speaker_details(speaker):
            continue
        attendees.append(
            {
                "fullName": speaker_name,
                "title": _first_string(speaker.get("role")),
                "linkedInUrl": _first_string(speaker.get("linkedin")),
                "relationshipType": "SPEAKER",
                "metadataJson": {"sourceProvider": "conferank", "speaker": speaker},
            }
        )

    sponsors = enriched.get("sponsors") if isinstance(enriched.get("sponsors"), list) else []
    companies: List[GenericCompanyDict] = []
    for sponsor in sponsors:
        if not isinstance(sponsor, dict):
            continue
        sponsor_name = _first_string(sponsor.get("name"))
        if not sponsor_name:
            continue
        companies.append(
            {
                "name": sponsor_name,
                "websiteUrl": _first_string(sponsor.get("website")),
                "relationshipType": _first_string(sponsor.get("tier"), "Sponsor"),
                "metadataJson": {"sourceProvider": "conferank", "sponsor": sponsor},
            }
        )

    occurrence: GenericOccurrenceDict = {
        "locationText": _first_string(venue_name, street_address, city),
        "latitude": _first_float(location.get("latitude"), enriched.get("latitude")),
        "longitude": _first_float(location.get("longitude"), enriched.get("longitude")),
        "venueName": venue_name,
        "streetAddress": street_address,
        "city": city,
        "country": country,
    }
    occurrence = {key: value for key, value in occurrence.items() if value not in (None, "", [], {})}  # type: ignore

    category = _first_string(*(categories or []))
    industry = ", ".join(str(item).strip() for item in categories if str(item).strip()) or category
    metadata = {
        **raw_event,
        "sourceProvider": "conferank",
        "offers": offers,
    }

    event: GenericMappedEventDict = {
        "name": name,
        "sourceUrl": _first_string(raw_event.get("url"), raw_event.get("source_url"), raw_event.get("sourceUrl")),
        "startAt": _first_string(raw_event.get("startDate")),
        "endAt": _first_string(raw_event.get("endDate")),
        "city": city,
        "country": country,
        "eventType": _first_string(raw_event.get("eventType"), category, "Conference"),
        "category": category,
        "eventImageUrl": _first_string(raw_event.get("image"), raw_event.get("eventImageUrl")),
        "industry": industry,
        "description": _first_string(raw_event.get("description")),
        "sourceProvider": "conferank",
        "attendees": attendees,
        "companies": companies,
        "occurrence": occurrence,
        "metadataJson": metadata,
    }
    return {key: value for key, value in event.items() if value not in (None, "", [], {})}  # type: ignore

async def ingest_conferank_events_to_eagle(
    *,
    organization_id: Optional[str] = None,
    workspace_id: Optional[str] = None,
    events: List[Dict[str, Any]],
    parse_failures: Optional[List[Dict[str, Any]]] = None,
    diagnostics: Optional[Dict[str, Any]] = None,
    persist: bool = False,
) -> Dict[str, Any]:
    response = await ingest_generic_events_to_eagle(
        organization_id=organization_id,
        workspace_id=workspace_id,
        events=events,
        source_provider="conferank",
        parse_failures=parse_failures,
        persist=persist,
        already_mapped=True,
    )
    if diagnostics:
        response["diagnostics"] = diagnostics
    return response

if __name__ == "__main__":
    import asyncio
    async def test():
        events = await crawl_conferank_events(limit=100, enrich_details=False)
        blackhat = next((ev for ev in events if 'blackhat' in (ev.get('sourceUrl') or '').lower()), None)
        
        if blackhat:
            print(json.dumps([blackhat], indent=2, ensure_ascii=False))
        else:
            print("Không tìm thấy Black Hat trong 100 sự kiện đầu tiên.")
    asyncio.run(test())
