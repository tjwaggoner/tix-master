# Ticketmaster API Information

## Rate Limits & Constraints

### Official Ticketmaster API Limits

| Limit Type | Value |
|------------|-------|
| **Requests per second** | 5 requests/sec (hard limit) |
| **Requests per day** | 5,000 requests/day (default quota) |
| **Pagination constraint** | `size × page < 1000` |

### Our Configuration

```python
PAGE_SIZE = 200        # Records per request
MAX_PAGES = 4          # Maximum pages per date chunk
Total per chunk = 800  # 200 × 4 = 800 (stays under 1000 limit)
```

## Key Concepts

### Request vs Record

- **Single request** = One API call to Ticketmaster
- **Single record** = One event, venue, or attraction object

**Example:**
```
1 API request with size=200 → Returns 200 event records
```

### Page Size

**Page size** = Number of records returned in a single API request

**IMPORTANT: Undocumented Maximum Page Size**

Testing revealed that Ticketmaster has an **undocumented maximum page size of ~200**:
- `size=200`: Returns data successfully ✓
- `size=500`: Returns 0 events ✗
- `size=1000`: Returns 0 events ✗

While the official documentation states `size × page < 1000`, the API silently rejects or caps requests with `size > 200`.

**Trade-offs:**

| Page Size | API Calls (for 1000 records) | Speed | Memory | Works? |
|-----------|------------------------------|-------|--------|--------|
| 50 | 20 requests | Slower | Lower | ✓ |
| 200 (ours) | 5 requests | Faster | Moderate | ✓ |
| 500 | 2 requests | Fastest | Higher | ✗ No data |
| 1000 | 1 request | Fastest | Highest | ✗ No data |

**Why we chose 200:**
- **Maximum supported page size** based on API testing
- Minimizes API calls (5x fewer than size=50)
- Stays safely under pagination limit (200×4=800 < 1000)
- Respects rate limits (fewer requests = less likely to hit 5/sec limit)

## Example Event Structure

```json
{
  "id": "G5vYZ9AE7rKjl",
  "name": "Taylor Swift | The Eras Tour",
  "type": "event",
  "url": "https://www.ticketmaster.com/event/G5vYZ9AE7rKjl",

  "dates": {
    "start": {
      "localDate": "2024-03-15",
      "localTime": "19:00:00",
      "dateTime": "2024-03-16T02:00:00Z"
    },
    "timezone": "America/Los_Angeles",
    "status": {
      "code": "onsale"
    }
  },

  "classifications": [
    {
      "segment": { "id": "KZFzniwnSyZfZ7v7nJ", "name": "Music" },
      "genre": { "id": "KnvZfZ7vAeA", "name": "Rock" },
      "subGenre": { "id": "KZazBEonSMnZfZ7v6F1", "name": "Pop" }
    }
  ],

  "priceRanges": [
    {
      "type": "standard",
      "currency": "USD",
      "min": 49.50,
      "max": 499.50
    }
  ],

  "_embedded": {
    "venues": [
      {
        "id": "KovZpZAEdFtJ",
        "name": "SoFi Stadium",
        "city": { "name": "Inglewood" },
        "state": { "name": "California", "stateCode": "CA" },
        "country": { "name": "United States", "countryCode": "US" },
        "location": {
          "longitude": "-118.33800630",
          "latitude": "33.95356270"
        }
      }
    ],
    "attractions": [
      {
        "id": "K8vZ917Gku7",
        "name": "Taylor Swift",
        "classifications": [
          { "genre": { "name": "Pop" } }
        ]
      }
    ]
  }
}
```

## Performance Metrics

Based on historical ingestion run (2 years back + 1 year forward):

| Metric | Value |
|--------|-------|
| Total Records | 12,861 |
| Total Data Size | 93.11 MB |
| Duration | 48.1 seconds |
| Data Throughput | 1.94 MB/s |
| Record Throughput | 267 records/second |
| API Calls | ~64 requests |

## Optimization Strategy

### Monthly Chunking
Events are fetched in monthly date ranges to avoid pagination limits:
- Single date range might exceed 1000 events
- Solution: Query month-by-month, each chunk stays under limit
- Implemented in `generate_monthly_chunks()` function

### Endpoint-Specific Approaches
- **Events**: Monthly chunks with date filters (largest dataset)
- **Venues/Attractions/Classifications**: Single batch (smaller datasets)

## Resources

- [Ticketmaster Discovery API Documentation](https://developer.ticketmaster.com/products-and-docs/apis/discovery-api/v2/)
- [Getting Started Guide](https://developer.ticketmaster.com/products-and-docs/apis/getting-started/)
