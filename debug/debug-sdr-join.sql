-- Debug the JOIN in event_map creation
SELECT
  le.id AS code,
  le.name AS base_name,
  sce.code AS sdr_code,
  sce.name_override AS sdr_name,
  COALESCE(sce.name_override, sev.name_override, le.name) AS final_name
FROM `mixpanel-gtm-training.korn_ferry_adobe_main.lookup_events` le
LEFT JOIN `mixpanel-gtm-training.korn_ferry_adobe_main.sdr_custom_events` sce ON le.id = sce.code
LEFT JOIN `mixpanel-gtm-training.korn_ferry_adobe_main.sdr_evars` sev ON le.id = sev.code
WHERE le.id IN (204, 211, 213, 232)
ORDER BY le.id;