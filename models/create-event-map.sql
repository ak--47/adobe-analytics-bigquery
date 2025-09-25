-- Create event mapping table combining base events with SDR overrides
-- This implements the two-layer override system mentioned in the research
CREATE OR REPLACE TABLE `${project}.${dataset}.event_map` AS
WITH
-- Base event codes from events.tsv
base_events AS (
  SELECT
    SAFE_CAST(id AS INT64) AS code,
    name
  FROM `${project}.${dataset}.lookup_events`
),

-- Custom events from SDR (event1-N maps to codes 200+N)
sdr_custom_events AS (
  SELECT
    CASE
      WHEN REGEXP_EXTRACT(Event, r'event(\d+)') IS NOT NULL
      THEN 200 + SAFE_CAST(REGEXP_EXTRACT(Event, r'event(\d+)') AS INT64)
      ELSE NULL
    END AS code,
    Name AS name
  FROM `${project}.${dataset}.sdr_custom_events_raw`
  WHERE REGEXP_EXTRACT(Event, r'event(\d+)') IS NOT NULL
    AND Name IS NOT NULL
    AND Name != ''
),

-- eVar instances from SDR (evar1-100 → codes 100-199, evar101+ → codes 10000+)
sdr_evar_instances AS (
  SELECT
    CASE
      WHEN REGEXP_EXTRACT(EvarNumber, r'evar(\d+)') IS NOT NULL THEN
        CASE
          WHEN SAFE_CAST(REGEXP_EXTRACT(EvarNumber, r'evar(\d+)') AS INT64) BETWEEN 1 AND 100
          THEN 99 + SAFE_CAST(REGEXP_EXTRACT(EvarNumber, r'evar(\d+)') AS INT64)  -- evar1 → 100, evar2 → 101, etc.
          WHEN SAFE_CAST(REGEXP_EXTRACT(EvarNumber, r'evar(\d+)') AS INT64) > 100
          THEN 9999 + SAFE_CAST(REGEXP_EXTRACT(EvarNumber, r'evar(\d+)') AS INT64) -- evar101 → 10100, etc.
          ELSE NULL
        END
      ELSE NULL
    END AS code,
    CONCAT('Instance of ', Name) AS name
  FROM `${project}.${dataset}.sdr_evars_raw`
  WHERE REGEXP_EXTRACT(EvarNumber, r'evar(\d+)') IS NOT NULL
    AND Name IS NOT NULL
    AND Name != ''
),

-- Combine all mappings with precedence ranking
all_events AS (
  SELECT code, name, 'base' AS source, 3 AS priority FROM base_events
  UNION ALL
  SELECT code, name, 'sdr_evar' AS source, 2 AS priority FROM sdr_evar_instances
  UNION ALL
  SELECT code, name, 'sdr_custom' AS source, 1 AS priority FROM sdr_custom_events
),

-- Select the highest priority mapping for each code
ranked_events AS (
  SELECT
    code,
    name,
    source,
    priority,
    ROW_NUMBER() OVER (PARTITION BY code ORDER BY priority ASC) AS rn
  FROM all_events
  WHERE code IS NOT NULL
)

-- Final event map with proper precedence (SDR overrides base)
SELECT
  code,
  name,
  source AS mapping_source
FROM ranked_events
WHERE rn = 1
ORDER BY code;