# Schema Retention Analysis: Raw API JSON → Gold Tables

## Summary Statistics

| Layer | Total Fields | Retention Rate |
|-------|--------------|----------------|
| **Raw API (Bronze)** | 52 unique field paths | 100% (baseline) |
| **Silver Layer** | 47 extracted fields | 90.4% |
| **Gold Layer** | 46 final fields | 88.5% |

## Field-by-Field Tracking

### Events Entity

#### Raw API Fields (from Ticketmaster Events endpoint)
Total: 21 fields

| Field Path | Bronze | Silver | Gold | Status |
|------------|--------|--------|------|--------|
| `id` | ✓ | ✓ event_id | ✓ event_id | **Retained** |
| `name` | ✓ | ✓ event_name | ✓ event_name | **Retained** |
| `type` | ✓ | ✓ event_type | ✓ event_type | **Retained** |
| `url` | ✓ | ✓ event_url | ✓ event_url | **Retained** |
| `locale` | ✓ | ✓ locale | ✗ | **Lost in Gold** |
| `info` | ✓ | ✓ event_info | ✗ | **Lost in Gold** |
| `pleaseNote` | ✓ | ✓ please_note | ✗ | **Lost in Gold** |
| `test` | ✓ | ✓ is_test | ✓ is_test | **Retained** |
| `dates.start.localDate` | ✓ | ✓ event_date | → date_sk_fk | **Retained (transformed to FK)** |
| `dates.start.localTime` | ✓ | ✓ event_time | ✓ event_time | **Retained** |
| `dates.start.dateTime` | ✓ | ✓ event_datetime | ✓ event_datetime | **Retained** |
| `dates.timezone` | ✓ | ✓ event_timezone | ✓ event_timezone | **Retained** |
| `dates.status.code` | ✓ | ✓ status_code | ✓ status_code | **Retained** |
| `priceRanges[0].type` | ✓ | ✗ | ✗ | **Lost in Silver** |
| `priceRanges[0].min` | ✓ | ✓ price_min | ✓ price_min | **Retained** |
| `priceRanges[0].max` | ✓ | ✓ price_max | ✓ price_max | **Retained** |
| `priceRanges[0].currency` | ✓ | ✓ price_currency | ✓ price_currency | **Retained** |
| `sales.public.startDateTime` | ✓ | ✓ sales_start_datetime | ✓ sales_start_datetime | **Retained** |
| `sales.public.endDateTime` | ✓ | ✓ sales_end_datetime | ✓ sales_end_datetime | **Retained** |
| `classifications[0].segment.id` | ✓ | ✓ segment_id | → classification_sk_fk | **Retained (normalized to dim)** |
| `classifications[0].genre.id` | ✓ | ✓ genre_id | → classification_sk_fk | **Retained (normalized to dim)** |

**Events Retention:** 18/21 fields retained to Gold (85.7%)

---

### Venues Entity

#### Raw API Fields (from Ticketmaster Venues endpoint + _embedded.venues)
Total: 15 fields

| Field Path | Bronze | Silver | Gold | Status |
|------------|--------|--------|------|--------|
| `id` | ✓ | ✓ venue_id | ✓ venue_id | **Retained** |
| `name` | ✓ | ✓ venue_name | ✓ venue_name | **Retained** |
| `type` | ✓ | ✓ venue_type | ✓ venue_type | **Retained** |
| `url` | ✓ | ✓ venue_url | ✓ venue_url | **Retained** |
| `locale` | ✓ | ✓ locale | ✗ | **Lost in Gold** |
| `postalCode` | ✓ | ✓ postal_code | ✓ postal_code | **Retained** |
| `timezone` | ✓ | ✓ timezone | ✓ timezone | **Retained** |
| `city.name` | ✓ | ✓ city | ✓ city | **Retained** |
| `state.name` | ✓ | ✓ state | ✓ state | **Retained** |
| `state.stateCode` | ✓ | ✓ state_code | ✓ state_code | **Retained** |
| `country.name` | ✓ | ✓ country | ✓ country | **Retained** |
| `country.countryCode` | ✓ | ✓ country_code | ✓ country_code | **Retained** |
| `address.line1` | ✓ | ✓ address_line1 | ✓ address_line1 | **Retained** |
| `location.latitude` | ✓ | ✓ latitude | ✓ latitude | **Retained** |
| `location.longitude` | ✓ | ✓ longitude | ✓ longitude | **Retained** |

**Venues Retention:** 14/15 fields retained to Gold (93.3%)

---

### Attractions Entity

#### Raw API Fields (from Ticketmaster Attractions endpoint + _embedded.attractions)
Total: 10 fields

| Field Path | Bronze | Silver | Gold | Status |
|------------|--------|--------|------|--------|
| `id` | ✓ | ✓ attraction_id | ✓ attraction_id | **Retained** |
| `name` | ✓ | ✓ attraction_name | ✓ attraction_name | **Retained** |
| `type` | ✓ | ✓ attraction_type | ✓ attraction_type | **Retained** |
| `url` | ✓ | ✓ attraction_url | ✓ attraction_url | **Retained** |
| `locale` | ✓ | ✓ locale | ✗ | **Lost in Gold** |
| `test` | ✓ | ✓ is_test | ✓ is_test | **Retained** |
| `classifications[0].segment.id` | ✓ | ✓ segment_id | ✓ segment_id | **Retained** |
| `classifications[0].segment.name` | ✓ | ✓ segment_name | ✓ segment_name | **Retained** |
| `classifications[0].genre.id` | ✓ | ✓ genre_id | ✓ genre_id | **Retained** |
| `classifications[0].genre.name` | ✓ | ✓ genre_name | ✓ genre_name | **Retained** |

**Attractions Retention:** 9/10 fields retained to Gold (90.0%)

---

### Classifications Entity

#### Raw API Fields (from Ticketmaster Classifications endpoint)
Total: 11 fields

| Field Path | Bronze | Silver | Gold | Status |
|------------|--------|--------|------|--------|
| `segment.id` | ✓ | ✓ segment_id | ✓ segment_id | **Retained** |
| `segment.name` | ✓ | ✓ segment_name | ✓ segment_name | **Retained** |
| `genre.id` | ✓ | ✓ genre_id | ✓ genre_id | **Retained** |
| `genre.name` | ✓ | ✓ genre_name | ✓ genre_name | **Retained** |
| `subGenre.id` | ✓ | ✓ subgenre_id | ✓ subgenre_id | **Retained** |
| `subGenre.name` | ✓ | ✓ subgenre_name | ✓ subgenre_name | **Retained** |
| `type.id` | ✓ | ✓ type_id | ✓ type_id | **Retained** |
| `type.name` | ✓ | ✓ type_name | ✓ type_name | **Retained** |
| `subType.id` | ✓ | ✓ subtype_id | ✓ subtype_id | **Retained** |
| `subType.name` | ✓ | ✓ subtype_name | ✓ subtype_name | **Retained** |
| `family` | ✓ | ✓ is_family | ✗ | **Lost in Gold** |

**Classifications Retention:** 10/11 fields retained to Gold (90.9%)

---

### Markets Entity

#### Raw API Fields (from _embedded.venues[].markets)
Total: 2 fields

| Field Path | Bronze | Silver | Gold | Status |
|------------|--------|--------|------|--------|
| `markets[].id` | ✓ | ✓ market_id | ✓ market_id | **Retained** |
| `markets[].name` | ✓ | ✓ market_name | ✓ market_name | **Retained** |

**Markets Retention:** 2/2 fields retained to Gold (100%)

---

## Fields Lost in Pipeline

### Lost Between Bronze → Silver (1 field)

1. **`priceRanges[0].type`** (Events)
   - **Why:** Only min, max, and currency extracted; type field (e.g., "standard") not deemed necessary
   - **Impact:** Minor - price type categorization unavailable

### Lost Between Silver → Gold (5 fields)

1. **`locale`** (Events, Venues, Attractions)
   - **Why:** Not included in gold star schema dimensions/facts
   - **Impact:** Minor - language/locale information not available for analysis
   - **Count:** 3 occurrences across entities

2. **`event_info`** (Events)
   - **Why:** Free-text field not included in gold fact table
   - **Impact:** Minor - descriptive event information unavailable

3. **`please_note`** (Events)
   - **Why:** Free-text field not included in gold fact table
   - **Impact:** Minor - additional event notes unavailable

4. **`is_family`** (Classifications)
   - **Why:** Not included in dim_classification gold table
   - **Impact:** Minor - family-friendly flag unavailable

---

## Generated/Derived Fields (Not in Raw API)

These fields are created during transformation and add value:

### Silver Layer
- `venue_sk`, `attraction_sk`, `event_sk` - MD5 hash-based surrogate keys
- `classification_id` - SHA256 composite key

### Gold Layer
- **SCD Type 2 Tracking Fields:**
  - `valid_from`, `valid_to`, `is_current` (all dimensions)
- **Date Dimension Fields:**
  - `date_sk` - Auto-incrementing surrogate key
  - `date_key`, `year`, `month`, `day`, `quarter`, `week_of_year`
  - `day_of_week`, `month_name`, `day_name`, `is_weekend`, `month_end_date`
- **Foreign Keys:**
  - `date_sk_fk`, `event_date_fk`, `venue_sk_fk`, `attraction_sk_fk`, `classification_sk_fk`

---

## Retention Summary by Layer

### Bronze → Silver: 90.4% Retention
- **Lost:** 1 field (`priceRanges[0].type`)
- **Reason:** Intentional - field not needed for analytics

### Silver → Gold: 97.9% Retention
- **Lost:** 5 fields (`locale` x3, `event_info`, `please_note`, `is_family`)
- **Reason:** Free-text or non-essential fields excluded from star schema

### Overall Bronze → Gold: 88.5% Retention
- **Total lost:** 6 fields out of 52
- **Total retained:** 46 fields (88.5%)

---

## Key Observations

1. **High Retention Rate:** 88.5% of raw API fields make it to the gold layer
2. **Strategic Losses:** Lost fields are primarily:
   - Free-text descriptive fields (event_info, please_note)
   - Locale/language metadata (locale x3)
   - Non-critical categorization (priceRanges.type, is_family)
3. **Value-Add Transformations:**
   - Added 20+ derived fields (SCD Type 2, date dimension attributes)
   - Normalized data across 5 dimension tables + 1 fact table
4. **Data Completeness:** All critical business attributes retained:
   - Event identifiers, names, dates, prices
   - Venue locations and addresses
   - Attraction and classification details
   - Market information

## Recommendations

### Fields to Consider Recovering
If analytics use cases require:
- **`locale`**: Add to dimensions if multi-language analysis needed
- **`event_info`**: Add to fact_events if event descriptions are valuable
- **`priceRanges[0].type`**: Add if price categorization (standard/premium) is needed

### Current State: Production Ready
The current schema retains all essential fields for:
- Event trend analysis
- Venue performance metrics
- Attraction popularity tracking
- Geographic and temporal analysis
- Price analytics
