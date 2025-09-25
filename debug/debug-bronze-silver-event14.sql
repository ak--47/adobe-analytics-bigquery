-- Debug the Bronze → Silver transformation for event14

-- Check Bronze layer
SELECT
  'Bronze Layer' as layer,
  COUNT(*) as total_hits,
  COUNT(CASE WHEN EXISTS(
    SELECT 1 FROM UNNEST(events_enhanced) as event
    WHERE SAFE_CAST(event.event_code AS INT64) IN (213, 214)
  ) THEN 1 END) as hits_with_event14_codes
FROM `mixpanel-gtm-training.korn_ferry_adobe_main.kfmain_bronze`

UNION ALL

-- Check which event codes are actually in Bronze events_enhanced
SELECT
  'Bronze Event Codes' as layer,
  NULL as total_hits,
  COUNT(DISTINCT SAFE_CAST(event.event_code AS INT64)) as hits_with_event14_codes
FROM `mixpanel-gtm-training.korn_ferry_adobe_main.kfmain_bronze`,
UNNEST(events_enhanced) as event
WHERE SAFE_CAST(event.event_code AS INT64) BETWEEN 210 AND 220

UNION ALL

-- Sample Bronze events_enhanced for debugging
SELECT
  'Bronze Sample' as layer,
  COUNT(*) as total_hits,
  NULL as hits_with_event14_codes
FROM (
  SELECT event.event_code, event.event_name
  FROM `mixpanel-gtm-training.korn_ferry_adobe_main.kfmain_bronze`,
  UNNEST(events_enhanced) as event
  WHERE SAFE_CAST(event.event_code AS INT64) BETWEEN 210 AND 220
  LIMIT 10
)

ORDER BY layer;