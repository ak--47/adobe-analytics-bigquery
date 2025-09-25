-- Sample queries to validate the Adobe Analytics pipeline
-- These queries test the eventification and denormalization

-- 1. Monthly unique visitors (UTC)
WITH hits AS (
  SELECT DATE_TRUNC(DATE(ts_utc), MONTH) AS month_utc, distinct_id
  FROM `${project}.${dataset}.${goldTable}`
  WHERE ts_utc IS NOT NULL
)
SELECT month_utc, COUNT(DISTINCT distinct_id) AS unique_visitors
FROM hits
GROUP BY month_utc
ORDER BY month_utc;

-- 2. Monthly page views (UTC), optional by country
SELECT
  DATE_TRUNC(DATE(ts_utc), MONTH) AS month_utc,
  country_name,
  COUNTIF(is_page_view) AS page_views
FROM `${project}.${dataset}.${goldTable}`
GROUP BY month_utc, country_name
ORDER BY month_utc, page_views DESC;

-- 3. Top-5 countries per month
WITH pv AS (
  SELECT
    DATE_TRUNC(DATE(ts_utc), MONTH) AS month_utc,
    country_name,
    COUNTIF(is_page_view) AS page_views
  FROM `${project}.${dataset}.${goldTable}`
  GROUP BY month_utc, country_name
),
ranked AS (
  SELECT *,
    ROW_NUMBER() OVER (PARTITION BY month_utc ORDER BY page_views DESC) AS rn
  FROM pv
)
SELECT month_utc, country_name, page_views
FROM ranked
WHERE rn <= 5
ORDER BY month_utc, page_views DESC;

-- 4. Event frequency analysis
SELECT
  event,
  COUNT(*) as event_count,
  COUNT(DISTINCT distinct_id) as unique_users,
  ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) as percentage
FROM `${project}.${dataset}.${goldTable}`
WHERE event != 'Page Viewed'  -- Exclude page views to focus on interactions
GROUP BY event
ORDER BY event_count DESC
LIMIT 20;

-- 5. User journey analysis - events per user
SELECT
  distinct_id,
  COUNT(DISTINCT DATE(ts_utc)) as active_days,
  COUNTIF(is_page_view) as page_views,
  COUNTIF(NOT is_page_view) as interactions,
  COUNT(DISTINCT event) as unique_event_types,
  MIN(ts_utc) as first_seen,
  MAX(ts_utc) as last_seen
FROM `${project}.${dataset}.${goldTable}`
GROUP BY distinct_id
HAVING page_views > 0
ORDER BY active_days DESC, page_views DESC
LIMIT 100;

-- 6. Eventification validation - compare hit counts across layers
SELECT
  'Raw' as layer,
  COUNT(*) as row_count,
  COUNT(DISTINCT distinct_id) as unique_visitors
FROM `${project}.${dataset}.${rawTable}`
WHERE post_cust_hit_time_gmt IS NOT NULL

UNION ALL

SELECT
  'Bronze' as layer,
  COUNT(*) as row_count,
  COUNT(DISTINCT distinct_id) as unique_visitors
FROM `${project}.${dataset}.${bronzeTable}`

UNION ALL

SELECT
  'Silver' as layer,
  COUNT(*) as row_count,
  COUNT(DISTINCT distinct_id) as unique_visitors
FROM `${project}.${dataset}.${silverTable}`

UNION ALL

SELECT
  'Gold' as layer,
  COUNT(*) as row_count,
  COUNT(DISTINCT distinct_id) as unique_visitors
FROM `${project}.${dataset}.${goldTable}`;

-- 7. SDR mapping effectiveness
SELECT
  'Total Events' as metric,
  COUNT(*) as count
FROM `${project}.${dataset}.event_map`

UNION ALL

SELECT
  'SDR Custom Event Overrides' as metric,
  COUNT(*) as count
FROM `${project}.${dataset}.sdr_custom_events`

UNION ALL

SELECT
  'SDR eVar Overrides' as metric,
  COUNT(*) as count
FROM `${project}.${dataset}.sdr_evars`;

-- 8. Data quality checks
SELECT
  'Valid Timestamps' as check_name,
  COUNTIF(ts_utc IS NOT NULL) as pass_count,
  COUNT(*) as total_count,
  ROUND(COUNTIF(ts_utc IS NOT NULL) * 100.0 / COUNT(*), 2) as pass_percentage
FROM `${project}.${dataset}.${goldTable}`

UNION ALL

SELECT
  'Valid Distinct IDs' as check_name,
  COUNTIF(distinct_id IS NOT NULL AND distinct_id != '') as pass_count,
  COUNT(*) as total_count,
  ROUND(COUNTIF(distinct_id IS NOT NULL AND distinct_id != '') * 100.0 / COUNT(*), 2) as pass_percentage
FROM `${project}.${dataset}.${goldTable}`

UNION ALL

SELECT
  'Page Views Properly Flagged' as check_name,
  COUNTIF(is_page_view = TRUE AND event = 'Page Viewed') as pass_count,
  COUNTIF(event = 'Page Viewed') as total_count,
  ROUND(COUNTIF(is_page_view = TRUE AND event = 'Page Viewed') * 100.0 / COUNTIF(event = 'Page Viewed'), 2) as pass_percentage
FROM `${project}.${dataset}.${goldTable}`;