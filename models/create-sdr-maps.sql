-- Create SDR-aware event name mapping tables
DECLARE proj STRING DEFAULT '${project}';
DECLARE ds STRING DEFAULT '${dataset}';

-- Normalize SDR custom events -> numeric custom_event_number and computed code (199 + N)
CREATE OR REPLACE TABLE `${project}.${dataset}.sdr_custom_events` AS
SELECT
  SAFE_CAST(REGEXP_EXTRACT(Event, r'(?i)event(\d+)') AS INT64) AS custom_event_number,
  Name AS name_override,
  SAFE_CAST(199 + SAFE_CAST(REGEXP_EXTRACT(Event, r'(?i)event(\d+)') AS INT64) AS INT64) AS code
FROM `${project}.${dataset}.sdr_custom_events_raw`
WHERE REGEXP_CONTAINS(Event, r'(?i)^event\d+$')
  AND Name IS NOT NULL
  AND TRIM(Name) != '';

-- Normalize SDR evars -> evar_number; produce standard (100+) and extended (10000+) codes
CREATE OR REPLACE TABLE `${project}.${dataset}.sdr_evars` AS
WITH base AS (
  SELECT SAFE_CAST(REGEXP_EXTRACT(EvarNumber, r'(\d+)') AS INT64) AS evar_number,
         Name AS name_override
  FROM `${project}.${dataset}.sdr_evars_raw`
  WHERE Name IS NOT NULL
)
SELECT
  CASE
    WHEN evar_number BETWEEN 1 AND 100 THEN 99 + evar_number
    WHEN evar_number >= 101 THEN 10000 + (evar_number - 101)
    ELSE NULL
  END AS code,
  name_override
FROM base
WHERE evar_number IS NOT NULL;

-- Final event map (base events, overridden by SDR where available)
CREATE OR REPLACE TABLE `${project}.${dataset}.event_map` AS
SELECT
  le.id AS code,
  COALESCE(sce.name_override,
           sev.name_override,
           le.name) AS name
FROM `${project}.${dataset}.lookup_events` le
LEFT JOIN `${project}.${dataset}.sdr_custom_events` sce ON le.id = sce.code
LEFT JOIN `${project}.${dataset}.sdr_evars` sev ON le.id = sev.code;