-- Debug Bronze layer post_event_list processing

-- Check Bronze layer for event14 in parsed format
SELECT
  'Bronze Layer' as layer,
  COUNT(*) as total_hits,
  COUNT(CASE WHEN post_event_list LIKE '%214%' THEN 1 END) as hits_with_raw_event14,
  COUNT(CASE WHEN EXISTS(
    SELECT 1 FROM UNNEST(post_event_list_parsed) as event
    WHERE SAFE_CAST(event.event_code AS INT64) = 214
  ) THEN 1 END) as hits_with_parsed_event14
FROM `mixpanel-gtm-training.korn_ferry_adobe_main.kfmain_bronze`

UNION ALL

-- Check what event codes are actually present in Bronze
SELECT
  'Bronze Event Distribution' as layer,
  NULL as total_hits,
  COUNT(CASE WHEN SAFE_CAST(event.event_code AS INT64) = 214 THEN 1 END) as hits_with_raw_event14,
  COUNT(DISTINCT SAFE_CAST(event.event_code AS INT64)) as hits_with_parsed_event14
FROM `mixpanel-gtm-training.korn_ferry_adobe_main.kfmain_bronze`,
UNNEST(post_event_list_parsed) as event
WHERE SAFE_CAST(event.event_code AS INT64) BETWEEN 210 AND 220

ORDER BY layer;