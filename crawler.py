import sys
import asyncio

# Fix for Windows encoding and asyncio issues - MUST BE AT THE VERY TOP
if sys.platform == 'win32':
    import io
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8')
    asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())

import os
import json
import re
import logging
import random
from typing import List, Optional
from datetime import datetime

from openai import AsyncOpenAI
from ddgs import DDGS
from crawl4ai import AsyncWebCrawler, BrowserConfig, CrawlerRunConfig, CacheMode
from schemas import WeddingInfo
from dotenv import load_dotenv

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

load_dotenv()
OPENROUTER_API_KEY = os.getenv("OPENROUTER_API_KEY")
llm_client = AsyncOpenAI(
    api_key=OPENROUTER_API_KEY,
    base_url="https://openrouter.ai/api/v1"
)
llm_semaphore = asyncio.Semaphore(5)

# ---------------------------------------------------------------------------
# URL validation - only real wedding event pages
# ---------------------------------------------------------------------------
WEDDING_PAGE_PATTERNS = [
    # Zola: /wedding/SLUG or /wedding/SLUG/event
    re.compile(r'zola\.com/wedding/[^/?]{3,}/?$'),
    re.compile(r'zola\.com/wedding/[^/?]{3,}/(event|home|schedule|travel)/?$'),
    # TheKnot: new UUID format /us/name-name-YYYY-MM-DD-UUID or old /us/name-mon-year
    re.compile(r'theknot\.com/us/[\w-]+-\d{4}-\d{2}-\d{2}-[a-f0-9-]{36}'),
    re.compile(r'theknot\.com/us/[\w-]+-\w{3}-\d{4}/?$'),
    # WithJoy: /SLUG or /SLUG/subpage
    re.compile(r'withjoy\.com/[a-z0-9_-]{3,}/?$'),
    re.compile(r'withjoy\.com/[a-z0-9_-]{3,}/(event|schedule|home|about|travel|wedding)/?$'),
    # AppyCouple & Say I Do
    re.compile(r'appycouple\.com/[^/?]{3,}/?$'),
    re.compile(r'sayi\.do/[^/?]{3,}/?$'),
]

NOISE_PATTERNS = [
    re.compile(r'/(blog|help|lp|wedding-ideas|gifts|product|expert-advice|find-a-couple|wedding-planning|save-the-date\?|questions|articles)/'),
    re.compile(r'minted\.(com|us)'),  # No public crawlable wedding pages
    re.compile(r'withjoy\.com/(blog|help|articles|pricing|features)/'),
    re.compile(r'theknot\.com/(wedding|vendor|idea|article|registry|checklist|budget)'),
]

def is_valid_wedding_page(url: str) -> bool:
    for p in NOISE_PATTERNS:
        if p.search(url):
            return False
    for p in WEDDING_PAGE_PATTERNS:
        if p.search(url):
            return True
    return False

def upgrade_to_event_page(url: str) -> str:
    """For Zola, prefer the /event sub-page which has more venue/time details."""
    m = re.match(r'(https://www\.zola\.com/wedding/[^/?]+)/?$', url)
    if m:
        return m.group(1) + '/event'
    return url

# ---------------------------------------------------------------------------
# Search
# ---------------------------------------------------------------------------
async def search_wedding_urls(location: str, years: str = "2026..2028") -> List[str]:
    curr_yr = datetime.now().year
    next_yr = curr_yr + 1
    
    queries = [
        f'site:zola.com/wedding {location} {curr_yr}',
        f'site:zola.com/wedding {location} {next_yr}',
        f'site:theknot.com/us {location} {curr_yr}',
        f'site:theknot.com/us {location} {next_yr}',
        f'site:withjoy.com wedding {location} {curr_yr}',
        f'site:withjoy.com wedding {location} {next_yr}',
        f'site:appycouple.com {location} {curr_yr}',
        f'site:appycouple.com {location} {next_yr}',
        f'site:sayi.do {location} {curr_yr}',
    ]

    urls = []
    logger.info(f"Searching for weddings in '{location}'...")
    
    def sync_search(q: str):
        with DDGS() as ddgs:
            return list(ddgs.text(q, max_results=10))

    loop = asyncio.get_event_loop()
    try:
        for query in queries:
            logger.info(f"Query: {query}")
            try:
                results = await loop.run_in_executor(None, sync_search, query)
                if results:
                    found = [r['href'] for r in results]
                    logger.info(f"  -> {len(found)} raw URLs")
                    urls.extend(found)
                else:
                    logger.warning(f"  -> No results")
            except Exception as e:
                logger.error(f"  -> Search error: {e}")
            
            # Delay to avoid search engine rate limits (429/403)
            await asyncio.sleep(random.uniform(2.0, 4.0))
    except Exception as e:
        logger.error(f"DDGS error: {e}")

    # Filter valid pages, then upgrade Zola pages to /event version
    valid = [u for u in urls if is_valid_wedding_page(u)]
    valid = [upgrade_to_event_page(u) for u in valid]
    unique = list(dict.fromkeys(valid))  # deduplicate while preserving order

    logger.info(f"Valid wedding page URLs: {len(unique)}")
    for u in unique:
        logger.info(f"  ✓ {u}")
    return unique

# ---------------------------------------------------------------------------
# Groq extraction from markdown
# ---------------------------------------------------------------------------
def extract_with_groq(markdown: str, url: str, location: str) -> Optional[WeddingInfo]:
    raise NotImplementedError("Use async_extract_with_groq instead")

async def async_extract_with_groq(markdown: str, url: str, location: str) -> Optional[WeddingInfo]:
    today_str = datetime.now().strftime("%B %d, %Y")
    prompt = f"""You are reading a personal wedding website page.
Today is {today_str}.

Extract wedding details from the content and return a JSON object with:
- couple_names: Names of the couple (e.g. "Emily & James" or "Nguyen Thi Mai and John Smith")
- date: Wedding date (e.g. "Saturday, June 14, 2026")
  * If you see a countdown like "365 days to go", calculate from today ({today_str})
  * If partial like "June 2026", use that
- time: Ceremony time if present (e.g. "3:00 PM"), else null
- venue: Venue name/address if present, else null
- location: City/country (e.g. "Hanoi, Vietnam"), else null

RULES:
- If this page is NOT about a specific couple's wedding (it's a blog, template, product listing), return {{"not_wedding": true}}
- If the wedding date has already clearly passed (it happened before {today_str}), return {{"past_wedding": true}}
- IMPORTANT: We ONLY want weddings happening in or around "{location}". If the event is clearly located somewhere else (e.g. different state/country), return {{"wrong_location": true}}. (Honeymoon funds for {location} don't count).
- Return only valid JSON. No markdown fences.

Page URL: {url}
Content:
{markdown[:5000]}
"""
    try:
        async with llm_semaphore:
            response = await llm_client.chat.completions.create(
                model="openai/gpt-oss-120b:free",
                messages=[{"role": "user", "content": prompt}],
                temperature=0,
                max_tokens=400,
            )
        raw = response.choices[0].message.content.strip()
        raw = re.sub(r'^```[a-z]*\n?', '', raw)
        raw = re.sub(r'\n?```$', '', raw)
        logger.info(f"  Groq: {raw[:200]}")

        data = json.loads(raw)
        if data.get("not_wedding") or data.get("past_wedding") or data.get("wrong_location"):
            logger.warning(f"  Not a valid future wedding in {location}, skipping")
            return None
        if not data.get("couple_names") and not data.get("date"):
            logger.warning(f"  No couple_names or date, skipping")
            return None

        data["source_url"] = url
        return WeddingInfo(**data)
    except json.JSONDecodeError as e:
        logger.error(f"  JSON parse error: {e}")
    except Exception as e:
        logger.error(f"  Groq error: {e}")
    return None

# ---------------------------------------------------------------------------
# Crawl a single URL and extract
# ---------------------------------------------------------------------------
async def crawl_one(crawler: AsyncWebCrawler, run_config: CrawlerRunConfig,
                    url: str, location: str) -> Optional[WeddingInfo]:
    try:
        logger.info(f"Crawling: {url}")
        result = await crawler.arun(url=url, config=run_config)
        if not result.success:
            logger.error(f"  ✗ {result.error_message}")
            return None

        markdown = result.markdown or ""
        logger.info(f"  ✓ {len(markdown)} chars")
        if len(markdown) < 100:
            logger.warning(f"  Page too short, likely blocked")
            return None

        return await async_extract_with_groq(markdown, url, location)
    except Exception as e:
        logger.error(f"  ✗ Exception: {e}")
        return None

# ---------------------------------------------------------------------------
# Main extract function - concurrent crawling
# ---------------------------------------------------------------------------
async def extract_wedding_data(urls: List[str], location: str = "", concurrency: int = 5) -> List[WeddingInfo]:
    if not urls:
        return []

    browser_config = BrowserConfig(headless=True, verbose=False, viewport_width=1920, viewport_height=1080)
    run_config = CrawlerRunConfig(cache_mode=CacheMode.BYPASS)

    logger.info(f"Crawling {len(urls)} URLs concurrently (batch size {concurrency})...")
    all_weddings = []

    async with AsyncWebCrawler(config=browser_config) as crawler:
        # Batch concurrency to avoid overwhelming targets
        for i in range(0, len(urls), concurrency):
            batch = urls[i:i + concurrency]
            tasks = [crawl_one(crawler, run_config, url, location) for url in batch]
            results = await asyncio.gather(*tasks, return_exceptions=False)
            for w in results:
                if w is not None:
                    all_weddings.append(w)
            # Small delay between batches
            if i + concurrency < len(urls):
                await asyncio.sleep(1)

    logger.info(f"Done. Total weddings extracted: {len(all_weddings)}")
    return all_weddings


if __name__ == "__main__":
    async def main():
        urls = await search_wedding_urls("Hanoi")
        print(f"\nFound {len(urls)} valid wedding URLs")
        if urls:
            weddings = await extract_wedding_data(urls[:5], location="Hanoi")
            for w in weddings:
                print(json.dumps(w.model_dump(), ensure_ascii=False, indent=2))

    asyncio.run(main())
