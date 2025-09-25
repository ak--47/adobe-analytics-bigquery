-- Debug Bronze layer events_array processing for event14

-- Check Bronze layer for event14 in events_array
SELECT
  'Bronze Layer' as layer,
  COUNT(*) as total_hits,
  COUNT(CASE WHEN EXISTS(
    SELECT 1 FROM UNNEST(events_array) as event
    WHERE SAFE_CAST(event.event_code AS INT64) = 214  -- Raw event14 = 214
  ) THEN 1 END) as hits_with_event14_214,
  COUNT(CASE WHEN EXISTS(
    SELECT 1 FROM UNNEST(events_array) as event
    WHERE SAFE_CAST(event.event_code AS INT64) = 213  -- SDR mapped event14 = 213
  ) THEN 1 END) as hits_with_event14_213
FROM `mixpanel-gtm-training.korn_ferry_adobe_main.kfmain_bronze`

UNION ALL

-- Check Silver layer for event14 in events_enhanced
SELECT
  'Silver Layer' as layer,
  COUNT(*) as total_hits,
  COUNT(CASE WHEN EXISTS(
    SELECT 1 FROM UNNEST(events_enhanced) as event
    WHERE SAFE_CAST(event.event_code AS INT64) = 214
  ) THEN 1 END) as hits_with_event14_214,
  COUNT(CASE WHEN EXISTS(
    SELECT 1 FROM UNNEST(events_enhanced) as event
    WHERE SAFE_CAST(event.event_code AS INT64) = 213
  ) THEN 1 END) as hits_with_event14_213
FROM `mixpanel-gtm-training.korn_ferry_adobe_main.kfmain_silver`

ORDER BY layer;