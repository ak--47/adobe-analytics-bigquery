-- Transform Bronze to Silver layer with SDR mappings
-- Applies Solution Design Reference column renaming and enhanced event processing
CREATE OR REPLACE TABLE `${project}.${dataset}.${silverTable}`
PARTITION BY DATE(ts_utc)
CLUSTER BY distinct_id, is_page_view
AS
WITH
-- Parse events into simplified form (avoid correlated subqueries with JOINs)
events_parsed AS (
  SELECT
    *,
    -- Simple events array without the problematic JOIN
    ARRAY(
      SELECT AS STRUCT
        event.event_code,
        event.event_value,
        CONCAT('Event ', event.event_code) AS event_name, -- Simple naming for now
        CASE
          WHEN SAFE_CAST(event.event_code AS INT64) BETWEEN 200 AND 299 THEN 'custom_event'
          WHEN SAFE_CAST(event.event_code AS INT64) BETWEEN 1 AND 199 THEN 'standard_event'
          WHEN SAFE_CAST(event.event_code AS INT64) >= 10000 THEN 'evar_instance'
          ELSE 'unknown'
        END AS event_type,
        CASE
          WHEN event.event_value IS NOT NULL AND event.event_value > 1 THEN TRUE
          ELSE FALSE
        END AS is_measurement
      FROM UNNEST(events_array) AS event
      WHERE event.event_code IS NOT NULL
    ) AS events_enhanced,

    -- Extract eVar instances from events for separate processing
    ARRAY(
      SELECT AS STRUCT
        event.event_code,
        event.event_value,
        SAFE_CAST(event.event_code AS INT64) - 9999 AS evar_number
      FROM UNNEST(events_array) AS event
      WHERE SAFE_CAST(event.event_code AS INT64) >= 10000
    ) AS evar_instances

  FROM `${project}.${dataset}.${bronzeTable}`
),

-- Apply SPA-friendly page view detection and enhanced business logic
url_changes AS (
  SELECT
    *,
    LAG(page_url) OVER (PARTITION BY distinct_id ORDER BY ts_utc) AS prev_url
  FROM events_parsed
),

silver_base AS (
  SELECT
    -- All original columns from Bronze (preserving full schema)
    * EXCEPT (events_array, events_enhanced, evar_instances), -- Replace events_array with enhanced version

    -- Enhanced events with SDR mappings
    events_enhanced,
    evar_instances,

    -- Simplified page view detection (avoid complex subqueries for now)
    CASE
      -- Primary: traditional page view beacon
      WHEN ${usePostPageEvent} AND (post_page_event = '0' OR SAFE_CAST(post_page_event AS INT64) = 0) THEN TRUE
      -- Fallback B: URL change within same visitor (SPA detection)
      WHEN ${useUrlChange} AND prev_url IS NOT NULL AND page_url IS NOT NULL AND prev_url != page_url THEN TRUE
      ELSE FALSE
    END AS is_page_view,

    -- Simplified event classification (avoid complex array analysis for now)
    CASE
      WHEN ARRAY_LENGTH(events_enhanced) > 0 THEN 'has_events'
      ELSE 'no_events'
    END AS event_classification,

    -- Dynamic SDR-based column aliases for eVars
${evarAliases}

    -- Dynamic SDR-based column aliases for Props
${propAliases}

  FROM url_changes
)

SELECT * FROM silver_base
WHERE ts_utc IS NOT NULL;