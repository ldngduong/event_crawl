# Eagle Event Crawler

FastAPI service for crawling public event sources and importing normalized events into Eagle.

## Run

```bash
./run_dev.sh
```

Default server:

```text
http://localhost:8006
```

## Common Endpoints

Humanitix ingest:

```bash
curl -X POST "http://localhost:8006/humanitix/events/ingest" \
  -H "Content-Type: application/json" \
  -d '{
    "location": "us--ny--new-york",
    "keyword": "conference",
    "source": "auto",
    "limit": 10,
    "persist": false
  }'
```

Meetup ingest:

```bash
curl -X POST "http://localhost:8006/meetup/events/ingest" \
  -H "Content-Type: application/json" \
  -d '{
    "search_url": "https://www.meetup.com/find/?location=us--ny--New+York&source=EVENTS&keywords=conference",
    "source": "auto",
    "limit": 10,
    "enrich_details": true,
    "persist": false
  }'
```

InternationalConferenceAlerts preview:

```bash
curl "http://localhost:8006/international-conference-alerts/events?search_url=https%3A%2F%2Finternationalconferencealerts.com%2Fconferences%3Fq%3Dtech%26country%3D%26month%3D&limit=10&source=auto"
```

Radius filter uses `lat`, `lng`, and `radius_km`. If an event lacks coordinates, the crawler tries `EAGLE_GEOCODING_URL` or defaults to `http://localhost:3001/api/v1/geocoding/address`.

```bash
curl "http://localhost:8006/international-conference-alerts/events?q=tech&limit=10&lat=40.7128&lng=-74.0060&radius_km=100"
```
