-- Debug event14 discrepancy between Silver and Gold layers

-- 1. Check Raw layer - post_event_list containing event14 (code 214 = 200+14)
SELECT
  'Raw Layer' as layer,
  COUNT(*) as total_hits,
  COUNT(CASE WHEN post_event_list LIKE '%214%' THEN 1 END) as hits_with_event14
FROM `mixpanel-gtm-training.korn_ferry_adobe_main.kfmain_raw`

UNION ALL

-- 2. Check Silver layer - events_enhanced array (should be code 213 = 199+14)
SELECT
  'Silver Layer' as layer,
  COUNT(*) as total_hits,
  COUNT(CASE WHEN EXISTS(
    SELECT 1 FROM UNNEST(events_enhanced) as event
    WHERE SAFE_CAST(event.event_code AS INT64) = 213
  ) THEN 1 END) as hits_with_event14
FROM `mixpanel-gtm-training.korn_ferry_adobe_main.kfmain_silver`

UNION ALL

-- 3. Check Gold layer - exploded events
SELECT
  'Gold Layer' as layer,
  COUNT(DISTINCT original_hit_no) as total_hits,
  COUNT(CASE WHEN event = 'Asset Download (Gated)' THEN 1 END) as hits_with_event14
FROM `mixpanel-gtm-training.korn_ferry_adobe_main.kfmain_gold`

ORDER BY layer;