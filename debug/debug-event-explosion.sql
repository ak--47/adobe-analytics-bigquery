-- Debug event14 discrepancy between Silver and Gold layers
-- Check how events are processed through the pipeline

-- 1. Check Raw layer - post_event_list containing event14
WITH raw_event14 AS (
  SELECT
    'Raw Layer' as layer,
    COUNT(*) as total_hits,
    COUNT(CASE WHEN post_event_list LIKE '%,214,%' OR post_event_list LIKE '%214,%' OR post_event_list LIKE '%,214' OR post_event_list = '214' THEN 1 END) as hits_with_event14
  FROM `mixpanel-gtm-training.korn_ferry_adobe_main.kfmain_raw`
),

-- 2. Check Silver layer - events_enhanced array
silver_event14 AS (
  SELECT
    'Silver Layer' as layer,
    COUNT(*) as total_hits,
    COUNT(CASE WHEN EXISTS(
      SELECT 1 FROM UNNEST(events_enhanced) as event
      WHERE SAFE_CAST(event.event_code AS INT64) = 213
    ) THEN 1 END) as hits_with_event14,
    -- Count actual event instances in the array
    SUM(ARRAY_LENGTH(ARRAY(
      SELECT event FROM UNNEST(events_enhanced) as event
      WHERE SAFE_CAST(event.event_code AS INT64) = 213
    ))) as total_event14_instances
  FROM `mixpanel-gtm-training.korn_ferry_adobe_main.kfmain_silver`
),

-- 3. Check Gold layer - exploded events
gold_event14 AS (
  SELECT
    'Gold Layer' as layer,
    COUNT(CASE WHEN event = 'Asset Download (Gated)' THEN 1 END) as total_event14_instances,
    COUNT(DISTINCT original_hit_no) as unique_hits_with_event14,
    COUNT(DISTINCT insert_id) as unique_event14_records
  FROM `mixpanel-gtm-training.korn_ferry_adobe_main.kfmain_gold`
)

-- Combine results
SELECT layer, total_hits, hits_with_event14, total_event14_instances
FROM raw_event14
UNION ALL
SELECT layer, total_hits, hits_with_event14, total_event14_instances
FROM silver_event14
UNION ALL
SELECT layer, NULL as total_hits, unique_hits_with_event14 as hits_with_event14, total_event14_instances
FROM gold_event14

UNION ALL

-- 4. Sample data comparison
SELECT 'Sample Check' as layer, NULL as total_hits, NULL as hits_with_event14, NULL as total_event14_instances

UNION ALL

-- Show a few sample hits that have event14 in different layers
SELECT
  'Sample Raw' as layer,
  NULL as total_hits,
  NULL as hits_with_event14,
  CAST(COUNT(*) AS INT64) as total_event14_instances
FROM (
  SELECT post_event_list
  FROM `mixpanel-gtm-training.korn_ferry_adobe_main.kfmain_raw`
  WHERE post_event_list LIKE '%214%'
  LIMIT 3
)

ORDER BY layer;