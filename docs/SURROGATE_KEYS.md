# Surrogate Key Documentation

This document describes the surrogate key composition for all dimension and fact tables in the Ticketmaster data pipeline.

## Overview

**Why Surrogate Keys?** Surrogate keys provide stable, business-driven identifiers for deduplication and joins, independent of API-provided IDs. They ensure that semantically identical records (same business attributes) are treated as the same entity, even if they have different API IDs.

**Key Design Principles:**
- All surrogate keys use MD5 hash of business attributes
- Fields are concatenated with `||` separator
- NULL values are coalesced to meaningful defaults (e.g., "NONE", "", "0", "1970-01-01")
- Primary Key constraints match the surrogate keys used in MERGE operations
- SCD Type 2 dimension tracking uses surrogate keys for proper history tracking

## Dimension Tables

### dim_venue (Silver: venues)

| Attribute | Value |
|-----------|-------|
| **Surrogate Key** | `venue_sk` |
| **Hash Function** | MD5 |
| **Source Fields** | `venue_id` |
| **Formula** | `MD5(venue_id)` |
| **NULL Handling** | N/A (venue_id is required, filtered before hash) |
| **PK Constraint** | `venues_pk` on `venue_sk` |
| **Natural Key** | `venue_id` (from API) |
| **Rationale** | Uses stable API identifier as hash source. Ensures SCD Type 2 tracks different versions of the same venue (same venue_sk across all versions). Prevents issues with name/coordinate changes creating duplicate venues. |

**Example:**
```python
.withColumn("venue_sk",
    md5(col("venue_id"))
)
```

### dim_attraction (Silver: attractions)

| Attribute | Value |
|-----------|-------|
| **Surrogate Key** | `attraction_sk` |
| **Hash Function** | MD5 |
| **Source Fields** | `attraction_name`, `segment_name` |
| **Formula** | `MD5(attraction_name || segment_name)` |
| **NULL Handling** | attraction_name → "", segment_name → "NONE" |
| **PK Constraint** | `attractions_pk` on `attraction_sk` |
| **Natural Key** | `attraction_id` (from API) |
| **Rationale** | Deduplicates attractions by name and segment (e.g., "Taylor Swift" in "Music" vs sports). Same performer/team in same segment = same entity. |

**Example:**
```python
.withColumn("attraction_sk",
    md5(concat_ws("||",
        coalesce(col("attraction_name"), lit("")),
        coalesce(col("segment_name"), lit("NONE"))
    ))
)
```

### dim_classification (Silver: classifications)

| Attribute | Value |
|-----------|-------|
| **Surrogate Key** | `classification_sk` |
| **Hash Function** | MD5 |
| **Source Fields** | `segment_name`, `type_name` |
| **Formula** | `MD5(segment_name || type_name)` |
| **NULL Handling** | segment_name → "NONE", type_name → "NONE" |
| **PK Constraint** | `classifications_pk` on `classification_sk` |
| **Natural Key** | `classification_id` (SHA2 composite of segment/genre/type IDs) |
| **Rationale** | Deduplicates classifications by business meaning. Same segment + type = same classification, regardless of genre or API IDs. |

**Example:**
```python
# Surrogate key based on business attributes
.withColumn("classification_sk",
    md5(concat_ws("||",
        coalesce(col("segment_name"), lit("NONE")),
        coalesce(col("type_name"), lit("NONE"))
    ))
)

# Natural key composite (for reference only, not used in joins)
.withColumn("classification_id",
    sha2(concat_ws("_", segment_id, genre_id, type_id), 256)
)
```

## Fact Tables

### fact_events (Silver: events)

| Attribute | Value |
|-----------|-------|
| **Surrogate Key** | `event_sk` |
| **Hash Function** | MD5 |
| **Source Fields** | `event_name`, `event_datetime` |
| **Formula** | `MD5(event_name || event_datetime)` |
| **NULL Handling** | event_name → "", event_datetime → "1970-01-01" |
| **PK Constraint** | `events_pk` on `event_sk` |
| **Natural Key** | `event_id` (from API) |
| **Rationale** | Deduplicates events by name and when they occur. Same event name at same time = same event. |

**Foreign Keys:**
- `venue_sk` → dim_venue.venue_sk (direct reference)
- `classification_sk` → dim_classification.classification_sk (computed same way as dimension)
- `date_sk_fk` → dim_date.date_sk (via event_date join in Gold layer)
- `classification_sk_fk` → dim_classification.classification_sk (via classification_sk join in Gold layer)

**Example:**
```python
.withColumn("event_sk",
    md5(concat_ws("||",
        coalesce(col("event_name"), lit("")),
        coalesce(col("event_datetime").cast("string"), lit("1970-01-01"))
    ))
)
```

## Bridge Tables

### bridge_event_attractions (Silver: event_attractions)

| Attribute | Value |
|-----------|-------|
| **Composite PK** | `event_sk_fk`, `attraction_sk_fk` |
| **Hash Function** | N/A (foreign keys from dimension tables) |
| **PK Constraint** | `event_attractions_pk` on `(event_sk_fk, attraction_sk_fk)` |
| **Natural Keys** | `event_id`, `attraction_id` (from API, preserved but not used in PK) |
| **Rationale** | Many-to-many resolution using surrogate keys ensures proper referential integrity. |

**Foreign Key Constraints:**
- `event_sk_fk` → events.event_sk
- `attraction_sk_fk` → attractions.attraction_sk

## Migration from Natural Keys

**Before (Incorrect):** Primary keys were on natural API IDs, but MERGE operations used surrogate keys.
```python
# WRONG - PK constraint didn't match merge key
add_primary_key_if_not_exists("venues", "venues_pk", ["venue_id"])  # ❌
merge_upsert(df, batch_id, "venues", merge_keys=["venue_sk"])       # ❌ Mismatch!
```

**After (Correct):** Primary keys match MERGE keys for proper data integrity.
```python
# CORRECT - PK constraint matches merge key
add_primary_key_if_not_exists("venues", "venues_pk", ["venue_sk"])  # ✅
merge_upsert(df, batch_id, "venues", merge_keys=["venue_sk"])       # ✅ Match!
```

## Impact on Gold Layer

All Gold layer dimension tables use the same surrogate keys from Silver:
- `gold.dim_venue` uses `venue_sk` from `silver.venues`
- `gold.dim_attraction` uses `attraction_sk` from `silver.attractions`
- `gold.dim_classification` uses `classification_sk` from `silver.classifications`

Gold fact table joins use surrogate keys:
```sql
-- fact_events joins to dim_classification via surrogate key
FROM silver.events e
LEFT JOIN gold.dim_classification c
  ON e.classification_sk = c.classification_sk
  AND c.is_current = TRUE
```

## Foreign Key Constraints

### Overview

All Gold layer fact and bridge tables have **foreign key constraints** to their related dimension tables. These constraints are informational (using RELY/RELY NOVALIDATE flags) and help with query optimization and documentation.

### Primary Key Strategy for SCD Type 2 Dimensions

**Design Decision:** Dimension primary keys are **single-column** (e.g., `venue_sk`), not composite.

**Why Single-Column PKs?**

To support foreign key constraints from fact and bridge tables, dimension PKs were changed from composite `(venue_sk, valid_from)` to single-column `(venue_sk)`:

**Before (Composite PK):**
```sql
PRIMARY KEY (venue_sk, valid_from)  -- ❌ Cannot be referenced by single FK column
```

**After (Single-Column PK):**
```sql
PRIMARY KEY (venue_sk)  -- ✅ Can be referenced by venue_sk_fk
```

**Trade-offs:**

| Aspect | Single-Column PK | Composite PK |
|--------|------------------|--------------|
| FK Constraints | ✅ Supported | ❌ Not possible with single FK column |
| Version Uniqueness | ⚠️ Not enforced | ✅ Enforced at PK level |
| Query Optimization | ✅ Query optimizer aware of relationships | ❌ No FK metadata |
| Documentation | ✅ FK metadata visible via DESCRIBE TABLE | ❌ Relationships undocumented |
| SCD Type 2 Support | ✅ Multiple versions allowed | ✅ Multiple versions allowed |

**Ideal Solution (Disabled):**
```sql
PRIMARY KEY (venue_sk),
UNIQUE (venue_sk, valid_from)  -- Would enforce version uniqueness
-- ❌ UNIQUE constraints are disabled in this workspace
```

### FK Constraints in Gold Layer

#### fact_events

Foreign keys referencing dimension tables:

| FK Column | References | Constraint Type | Description |
|-----------|------------|----------------|-------------|
| `date_sk_fk` | `dim_date(date_sk)` | RELY | Date dimension (not SCD Type 2) |
| `venue_sk_fk` | `dim_venue(venue_sk)` | RELY NOVALIDATE | Venue dimension (SCD Type 2) |
| `classification_sk_fk` | `dim_classification(classification_sk)` | RELY NOVALIDATE | Classification dimension (SCD Type 2) |

**SQL Definition:**
```sql
CREATE TABLE ticket_master.gold.fact_events (
  event_sk STRING NOT NULL,
  date_sk_fk INT NOT NULL,
  venue_sk_fk STRING NOT NULL,
  classification_sk_fk STRING NOT NULL,
  -- ... other columns ...

  CONSTRAINT fact_events_pk PRIMARY KEY (event_sk),
  CONSTRAINT fact_events_date_fk FOREIGN KEY (date_sk_fk)
    REFERENCES ticket_master.gold.dim_date(date_sk) RELY,
  CONSTRAINT fact_events_venue_fk FOREIGN KEY (venue_sk_fk)
    REFERENCES ticket_master.gold.dim_venue(venue_sk) RELY NOVALIDATE,
  CONSTRAINT fact_events_classification_fk FOREIGN KEY (classification_sk_fk)
    REFERENCES ticket_master.gold.dim_classification(classification_sk) RELY NOVALIDATE
);
```

#### bridge_event_attractions

Foreign keys referencing fact and dimension tables:

| FK Column | References | Constraint Type | Description |
|-----------|------------|----------------|-------------|
| `event_sk_fk` | `fact_events(event_sk)` | RELY | Fact table reference |
| `attraction_sk_fk` | `dim_attraction(attraction_sk)` | RELY NOVALIDATE | Attraction dimension (SCD Type 2) |

**SQL Definition:**
```sql
CREATE TABLE ticket_master.gold.bridge_event_attractions (
  event_sk_fk STRING NOT NULL,
  attraction_sk_fk STRING NOT NULL,
  -- ... other columns ...

  CONSTRAINT bridge_event_attractions_pk PRIMARY KEY (event_sk_fk, attraction_sk_fk),
  CONSTRAINT bridge_events_fk FOREIGN KEY (event_sk_fk)
    REFERENCES ticket_master.gold.fact_events(event_sk) RELY,
  CONSTRAINT bridge_attractions_fk FOREIGN KEY (attraction_sk_fk)
    REFERENCES ticket_master.gold.dim_attraction(attraction_sk) RELY NOVALIDATE
);
```

### Understanding RELY and RELY NOVALIDATE

**RELY**
- Tells the query optimizer that the FK relationship exists and is valid
- Optimizer can use this information for query planning and optimization
- Does **not** enforce referential integrity (no validation on insert/update)
- Used for `dim_date` FK (non-SCD dimension)

**RELY NOVALIDATE**
- Same benefits as RELY for query optimization
- Explicitly indicates that referential integrity is **not** enforced
- Used for SCD Type 2 dimensions where surrogate key is not unique
- Allows multiple dimension versions per surrogate key
- Queries must still filter `is_current = TRUE`

**Why Not Enforce?**
- SCD Type 2 dimensions have multiple versions per surrogate key
- Single-column PK allows multiple rows with same `venue_sk` (different `valid_from`)
- Enforced FK constraints would fail because surrogate key is not unique
- Referential integrity is maintained through ETL logic, not constraints

### Column Comments on Foreign Keys

All FK columns include detailed comments documenting:
1. Which table/column they reference
2. How the surrogate key is computed (MD5 formula)
3. For SCD Type 2: Required `is_current = TRUE` filter in joins

**View Column Comments:**
```sql
DESCRIBE TABLE ticket_master.gold.fact_events;
```

**Example Comments:**

**date_sk_fk:**
```
Foreign key to dim_date.date_sk (sequential INT).
Join directly without is_current filter.
```

**venue_sk_fk:**
```
Foreign key to dim_venue.venue_sk: MD5(venue_id).
SCD Type 2: Must filter is_current = TRUE in joins.
```

**classification_sk_fk:**
```
Foreign key to dim_classification.classification_sk: MD5(segment_name || type_name).
SCD Type 2: Must filter is_current = TRUE in joins.
```

**event_sk_fk (bridge table):**
```
Foreign key to fact_events.event_sk: MD5(event_name || event_datetime)
```

**attraction_sk_fk (bridge table):**
```
Foreign key to dim_attraction.attraction_sk: MD5(attraction_name || segment_name).
SCD Type 2: Must filter is_current = TRUE in joins.
```

### Table Comments

Fact and bridge tables include comments explaining FK constraint behavior:

**fact_events:**
```
Fact table for events. FK constraints to SCD Type 2 dimensions (venue, classification)
use RELY NOVALIDATE because surrogate keys are not unique across dimension history.
Queries MUST filter by is_current = TRUE when joining to SCD Type 2 dimensions.
Example: INNER JOIN dim_venue v ON f.venue_sk_fk = v.venue_sk AND v.is_current = TRUE
```

**bridge_event_attractions:**
```
Bridge table for many-to-many relationship between events and attractions. FK to
dim_attraction uses RELY NOVALIDATE because attraction_sk is not unique (SCD Type 2).
Queries MUST filter by is_current = TRUE when joining to dim_attraction.
Example: INNER JOIN dim_attraction a ON b.attraction_sk_fk = a.attraction_sk AND a.is_current = TRUE
```

### Query Examples with FK Constraints

**Joining Fact to Dimension (SCD Type 2):**
```sql
SELECT
  e.event_name,
  e.event_date,
  v.venue_name,
  v.city,
  v.state
FROM ticket_master.gold.fact_events e
INNER JOIN ticket_master.gold.dim_venue v
  ON e.venue_sk_fk = v.venue_sk
  AND v.is_current = TRUE  -- ⚠️ Required for SCD Type 2
WHERE e.is_test = FALSE
  AND e.event_date >= CURRENT_DATE();
```

**Joining via Bridge Table:**
```sql
SELECT
  e.event_name,
  a.attraction_name,
  a.segment_name
FROM ticket_master.gold.fact_events e
INNER JOIN ticket_master.gold.bridge_event_attractions b
  ON e.event_sk = b.event_sk_fk
INNER JOIN ticket_master.gold.dim_attraction a
  ON b.attraction_sk_fk = a.attraction_sk
  AND a.is_current = TRUE  -- ⚠️ Required for SCD Type 2
WHERE e.is_test = FALSE;
```

**Joining to Non-SCD Dimension:**
```sql
SELECT
  e.event_name,
  d.full_date,
  d.day_of_week,
  d.month_name
FROM ticket_master.gold.fact_events e
INNER JOIN ticket_master.gold.dim_date d
  ON e.date_sk_fk = d.date_sk
  -- No is_current filter needed - dim_date is not SCD Type 2
WHERE e.event_date = CURRENT_DATE();
```

### Verifying FK Constraints

**List all FK constraints:**
```sql
SELECT
  table_name,
  constraint_name,
  constraint_type
FROM system.information_schema.table_constraints
WHERE table_schema = 'gold'
  AND table_catalog = 'ticket_master'
  AND constraint_type = 'FOREIGN KEY'
ORDER BY table_name, constraint_name;
```

**View FK relationships:**
```sql
SHOW CREATE TABLE ticket_master.gold.fact_events;
SHOW CREATE TABLE ticket_master.gold.bridge_event_attractions;
```

**Test FK relationships work:**
```sql
-- Should return counts matching fact table
SELECT COUNT(*) as events_with_venues
FROM ticket_master.gold.fact_events f
JOIN ticket_master.gold.dim_venue v
  ON f.venue_sk_fk = v.venue_sk
  AND v.is_current = TRUE;

-- Should match total events in fact table
SELECT COUNT(*) FROM ticket_master.gold.fact_events;
```

## Testing Surrogate Key Logic

To verify surrogate key deduplication is working:

```sql
-- Check for duplicates in venues (should return 0 rows)
SELECT venue_sk, COUNT(*) as cnt
FROM ticket_master.silver.venues
GROUP BY venue_sk
HAVING COUNT(*) > 1;

-- Verify surrogate keys are being populated
SELECT
  COUNT(*) as total_records,
  COUNT(DISTINCT venue_sk) as unique_sks,
  COUNT(DISTINCT venue_id) as unique_ids,
  COUNT(CASE WHEN venue_sk IS NULL THEN 1 END) as null_sks
FROM ticket_master.silver.venues;
```

## References

- Silver Layer Implementation: `src/silver/silver_transformations.py`
- Gold Layer Implementation: `src/gold/gold_star_schema.py`
- Kimball Dimensional Modeling: https://www.kimballgroup.com/data-warehouse-business-intelligence-resources/kimball-techniques/dimensional-modeling-techniques/
