# Adobe Analytics Pipeline Architecture Validation

## Layer Design Verification

### Raw → Bronze → Silver: Column Preservation
- **Raw**: All TSV columns loaded (quote="" prevents column drift)
- **Bronze**: All Raw columns + parsed/computed columns (ts_utc, distinct_id, events_array)
- **Silver**: All Bronze columns + enhanced events with SDR mappings + business-friendly aliases

**Column Count Progression:**
```
Raw:    ~1189 columns (all TSV columns)
Bronze: ~1189 + 3 columns (adds ts_utc, distinct_id, events_array, events_list_clean)
Silver: ~1189 + 3 + ~20 columns (adds events_enhanced, evar_instances, is_page_view, SDR aliases)
```

### Gold: Optimization + Eventification
- **Column Optimization**: Selects only essential columns (removes ~50% redundant non-post columns)
- **Eventification**: Explodes Silver hits into individual event records
- **Row Multiplication**: One Silver hit → Multiple Gold events (Page View + Business Events)

**Transformation:**
```
Silver: 1 hit with 3 events → Gold: 4 rows (1 Page View + 3 Business Events)
```

## Data Flow Architecture

```
┌─────────────┐    ┌─────────────┐    ┌─────────────┐    ┌─────────────┐
│    RAW      │    │   BRONZE    │    │   SILVER    │    │    GOLD     │
│             │    │             │    │             │    │             │
│ • TSV Load  │───▶│ • Parse     │───▶│ • SDR Map   │───▶│ • Eventify  │
│ • quote=""  │    │ • Clean     │    │ • Business  │    │ • Optimize  │
│ • All Cols  │    │ • All Cols  │    │ • All Cols  │    │ • Key Cols  │
│             │    │   + 3       │    │   + 20      │    │   ~50%      │
└─────────────┘    └─────────────┘    └─────────────┘    └─────────────┘
      │                    │                    │                    │
   1189 cols           1192 cols           1212 cols              ~600 cols
   1M hits             1M hits             1M hits               3M events
```

## Key Optimizations Applied

### 1. Loading Parameters (Critical)
- `quote=""` prevents column drift from stray quotes
- `field_delimiter=\t` for proper TSV parsing
- `allow_jagged_rows=true` handles missing trailing columns

### 2. Adobe-Specific Logic
- Prefer `post_event_list` over `event_list` (Adobe-processed data)
- SDR event mapping: `event8` → code `207` → "Asset Download"
- eVar mapping: `evar1-100` → codes `100-199`, `evar101+` → codes `10000+`

### 3. Performance Optimizations
- **Silver/Gold**: Partitioned by `DATE(ts_utc)` for query performance
- **Gold**: Clustered by `distinct_id, event, is_page_view` for analytics queries
- **Column Reduction**: Only in Gold layer to maintain data lineage in Silver

### 4. Data Quality Checks
- Column drift detection via `accept_language` validation
- Event code mapping validation
- Timestamp parsing validation (10-digit vs 13-digit)

## Validation Queries

### Layer Row Count Verification
```sql
-- Validate row progression through pipeline layers
SELECT 'Raw' as layer, COUNT(*) as rows, COUNT(DISTINCT CONCAT(post_visid_high, '-', post_visid_low)) as visitors
FROM `{project}.{dataset}.{tables.raw}`
UNION ALL
SELECT 'Bronze' as layer, COUNT(*) as rows, COUNT(DISTINCT distinct_id) as visitors
FROM `{project}.{dataset}.{tables.bronze}`
UNION ALL
SELECT 'Silver' as layer, COUNT(*) as rows, COUNT(DISTINCT distinct_id) as visitors
FROM `{project}.{dataset}.{tables.silver}`
UNION ALL
SELECT 'Gold' as layer, COUNT(*) as rows, COUNT(DISTINCT distinct_id) as visitors
FROM `{project}.{dataset}.{tables.gold}`
ORDER BY
  CASE layer
    WHEN 'Raw' THEN 1
    WHEN 'Bronze' THEN 2
    WHEN 'Silver' THEN 3
    WHEN 'Gold' THEN 4
  END;
-- Expected: Gold > Silver = Bronze = Raw (due to eventification)
-- Visitor counts should be identical across all layers
```

### Column Count Verification
```sql
-- Validate column progression through pipeline layers
SELECT table_name, COUNT(*) as column_count
FROM `{project}.{dataset}`.INFORMATION_SCHEMA.COLUMNS
WHERE table_name IN ('{tables.raw}', '{tables.bronze}', '{tables.silver}', '{tables.gold}')
GROUP BY table_name
ORDER BY
  CASE table_name
    WHEN '{tables.raw}' THEN 1
    WHEN '{tables.bronze}' THEN 2
    WHEN '{tables.silver}' THEN 3
    WHEN '{tables.gold}' THEN 4
  END;
-- Expected: Silver > Bronze > Raw, Gold < Silver (optimization)
```

### Eventification Validation
```sql
-- Validate event explosion from Silver hits to Gold events
WITH event_expansion AS (
  SELECT
    DATE(ts_utc) as date_utc,
    COUNT(*) as silver_hits,
    COUNT(DISTINCT distinct_id) as silver_visitors
  FROM `{project}.{dataset}.{tables.silver}`
  WHERE ts_utc IS NOT NULL
  GROUP BY DATE(ts_utc)
),
gold_events AS (
  SELECT
    DATE(ts_utc) as date_utc,
    COUNT(*) as gold_events,
    COUNT(DISTINCT distinct_id) as gold_visitors,
    COUNT(DISTINCT hit_id_high, hit_id_low) as unique_hits,
    COUNT(DISTINCT CASE WHEN event = 'Page Viewed' THEN CONCAT(hit_id_high, '-', hit_id_low) END) as page_views,
    COUNT(CASE WHEN event != 'Page Viewed' THEN 1 END) as business_events
  FROM `{project}.{dataset}.{tables.gold}`
  WHERE ts_utc IS NOT NULL
  GROUP BY DATE(ts_utc)
)
SELECT
  s.date_utc,
  s.silver_hits,
  g.gold_events,
  g.unique_hits,
  g.page_views,
  g.business_events,
  ROUND(g.gold_events / s.silver_hits, 2) as expansion_ratio,
  CASE
    WHEN g.unique_hits = s.silver_hits THEN '✓ Hit preservation'
    ELSE '✗ Hit count mismatch'
  END as hit_validation,
  CASE
    WHEN s.silver_visitors = g.gold_visitors THEN '✓ Visitor preservation'
    ELSE '✗ Visitor count mismatch'
  END as visitor_validation
FROM event_expansion s
JOIN gold_events g ON s.date_utc = g.date_utc
ORDER BY s.date_utc DESC
LIMIT 10;
-- Expected: expansion_ratio > 1.0, hit_validation = ✓, visitor_validation = ✓
```

### Data Quality Validation
```sql
-- Validate data quality metrics across pipeline layers
WITH layer_stats AS (
  SELECT
    'Bronze' as layer,
    COUNT(*) as total_rows,
    COUNT(DISTINCT distinct_id) as unique_visitors,
    COUNT(CASE WHEN ts_utc IS NULL THEN 1 END) as null_timestamps,
    COUNT(CASE WHEN post_event_list IS NULL OR post_event_list = '' THEN 1 END) as null_events
  FROM `{project}.{dataset}.{tables.bronze}`

  UNION ALL

  SELECT
    'Silver' as layer,
    COUNT(*) as total_rows,
    COUNT(DISTINCT distinct_id) as unique_visitors,
    COUNT(CASE WHEN ts_utc IS NULL THEN 1 END) as null_timestamps,
    COUNT(CASE WHEN events_enhanced IS NULL OR events_enhanced = '' THEN 1 END) as null_events
  FROM `{project}.{dataset}.{tables.silver}`

  UNION ALL

  SELECT
    'Gold' as layer,
    COUNT(*) as total_rows,
    COUNT(DISTINCT distinct_id) as unique_visitors,
    COUNT(CASE WHEN ts_utc IS NULL THEN 1 END) as null_timestamps,
    COUNT(CASE WHEN event IS NULL OR event = '' THEN 1 END) as null_events
  FROM `{project}.{dataset}.{tables.gold}`
)
SELECT
  layer,
  total_rows,
  unique_visitors,
  null_timestamps,
  null_events,
  ROUND(100.0 * null_timestamps / total_rows, 2) as pct_null_timestamps,
  ROUND(100.0 * null_events / total_rows, 2) as pct_null_events
FROM layer_stats
ORDER BY
  CASE layer
    WHEN 'Bronze' THEN 1
    WHEN 'Silver' THEN 2
    WHEN 'Gold' THEN 3
  END;
-- Expected: Low percentages of null values, consistent visitor counts
```

## Architecture Benefits

 **Data Lineage**: Full traceability from Raw → Bronze → Silver → Gold
 **Incremental Processing**: Each layer can be rebuilt independently
 **Storage Optimization**: 50% space savings applied only in Gold
 **Query Performance**: Partitioning and clustering for analytics workloads
 **Business Readability**: SDR mappings make data human-readable
 **Event Analytics**: Proper eventification enables user journey analysis