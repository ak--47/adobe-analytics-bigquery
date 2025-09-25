-- Transform Silver to Gold layer with full eventification and column optimization
-- Explodes Adobe hits into individual event records with proper sequencing
-- Applies Adobe-specific column optimization (removes non-post columns to save ~50% space)
CREATE OR REPLACE TABLE `${project}.${dataset}.${goldTable}`
PARTITION BY DATE(ts_utc)
CLUSTER BY distinct_id, event_name, is_page_view
AS
WITH
-- Separate measurements from real events
events_classified AS (
  SELECT
    *,
    -- Extract measurement properties (events with values > 1 or special measurement events)
    STRUCT(
      -- Static measurement extractions (legacy pattern matching)
      (SELECT event.event_value FROM UNNEST(events_enhanced) AS event WHERE event.event_name LIKE '%Page Load Time%' LIMIT 1) AS page_load_time,
      (SELECT event.event_value FROM UNNEST(events_enhanced) AS event WHERE event.event_name LIKE '%Time on Page%' LIMIT 1) AS time_on_page,
      (SELECT event.event_value FROM UNNEST(events_enhanced) AS event WHERE event.event_name LIKE '%Scroll%' LIMIT 1) AS scroll_depth,
      (SELECT event.event_value FROM UNNEST(events_enhanced) AS event WHERE event.event_name LIKE '%Form Start%' OR event.event_name LIKE '%Form Initialize%' LIMIT 1) AS form_starts,
      (SELECT event.event_value FROM UNNEST(events_enhanced) AS event WHERE event.event_name LIKE '%Form Success%' OR event.event_name LIKE '%Form Completion%' LIMIT 1) AS form_completions,
      (SELECT event.event_value FROM UNNEST(events_enhanced) AS event WHERE event.event_name LIKE '%Video%' LIMIT 1) AS video_plays,
      (SELECT event.event_value FROM UNNEST(events_enhanced) AS event WHERE event.event_name LIKE '%Download%' LIMIT 1) AS downloads,
      (SELECT event.event_value FROM UNNEST(events_enhanced) AS event WHERE event.event_name LIKE '%External%' LIMIT 1) AS external_links,
      (SELECT event.event_value FROM UNNEST(events_enhanced) AS event WHERE event.event_name LIKE '%Link Click%' LIMIT 1) AS custom_link_clicks,

      -- Dynamic measurement extractions (from measurement_event_codes)
      (SELECT event.event_value FROM UNNEST(events_enhanced) AS event WHERE SAFE_CAST(event.event_code AS INT64) = 209 LIMIT 1) AS measurement_209_page_load_time,
      (SELECT event.event_value FROM UNNEST(events_enhanced) AS event WHERE SAFE_CAST(event.event_code AS INT64) = 236 LIMIT 1) AS measurement_236_page_scroll_75,
      (SELECT event.event_value FROM UNNEST(events_enhanced) AS event WHERE SAFE_CAST(event.event_code AS INT64) = 240 LIMIT 1) AS measurement_240
    ) AS measurements,

    -- Extract only true business events (exclude eVar instances which should be properties)
    ARRAY(
      SELECT AS STRUCT
        event.event_name,
        event.event_code,
        event.event_value,
        event.event_type
      FROM UNNEST(events_enhanced) AS event
      WHERE event.event_code IS NOT NULL
        AND event.event_name IS NOT NULL
        AND event.event_name != ''
        -- Exclude eVar instances (codes 100-199 and 10000+) - these should be properties, not events
        AND NOT (SAFE_CAST(event.event_code AS INT64) BETWEEN 100 AND 199)
        AND NOT (SAFE_CAST(event.event_code AS INT64) >= 10000)
        -- Exclude measurement events - these should be properties, not exploded events
        AND SAFE_CAST(event.event_code AS INT64) NOT IN UNNEST(${measurementEventCodes})
        -- Exclude ignored hits - these should be completely discarded
        AND SAFE_CAST(event.event_code AS INT64) NOT IN UNNEST(${ignoreHits})
    ) AS business_events

  FROM `${project}.${dataset}.${silverTable}`
),

-- Generate hit-level base properties
hit_base AS (
  SELECT
    *,
    -- Create unique hit identifier
    CONCAT(
      distinct_id, '-',
      CAST(UNIX_SECONDS(ts_utc) AS STRING), '-',
      CAST(ROW_NUMBER() OVER (PARTITION BY distinct_id, ts_utc ORDER BY ts_utc) AS STRING)
    ) AS hit_id,

    -- Generate sequential hit number for traceability
    ROW_NUMBER() OVER (ORDER BY ts_utc, distinct_id) AS original_hit_no,

    -- Determine primary event name for this hit
    CASE
      WHEN is_page_view THEN 'Page Viewed'
      WHEN ARRAY_LENGTH(business_events) > 0 THEN business_events[OFFSET(0)].event_name
      ELSE 'Action Tracked'
    END AS primary_event

  FROM events_classified
),

-- Explode into individual events with proper sequencing
events_exploded AS (
  -- Page View events (one per page view)
  SELECT
    TO_HEX(MD5(CONCAT(distinct_id, '-', 'Page Viewed', '-', CAST(UNIX_MICROS(ts_utc) AS STRING)))) AS insert_id,
    ts_utc,
    distinct_id,
    'Page Viewed' AS event_name,
    NULL AS original_event_code,
    NULL AS original_event_name,
    TRUE AS is_page_view,
    FALSE AS is_link_tracking,

    -- Page properties
    page_url,
    pagename AS page_name,
    COALESCE(pagename, page_url) AS page_title,

    -- Add all measurement properties to page view
    measurements.page_load_time,
    measurements.time_on_page,
    measurements.scroll_depth,
    measurements.form_starts,
    measurements.form_completions,
    measurements.video_plays,
    measurements.downloads,
    measurements.external_links,
    measurements.custom_link_clicks,

    -- Add dynamic measurement columns
    measurements.measurement_209_page_load_time,
    measurements.measurement_236_page_scroll_75,
    measurements.measurement_240,

    CAST(NULL AS FLOAT64) AS event_value,

    -- Visitor properties
    distinct_id AS visitor_id,
    visit_num,
    CASE
      WHEN SAFE_CAST(visit_num AS INT64) = 1 THEN 'New'
      ELSE 'Returning'
    END AS visitor_type,
    yearly_visitor,
    monthly_visitor,
    daily_visitor,
    hourly_visitor,

    -- Technology properties (denormalized with lookups)
    browser,
    COALESCE(browser_lookup.name, CONCAT('Browser ', browser)) AS browser_name,
    os,
    COALESCE(os_lookup.name, CONCAT('OS ', os)) AS operating_system_name,
    user_agent,
    c_color AS color_depth,
    javascript AS javascript_version,
    java_enabled,
    connection_type,
    COALESCE(connection_lookup.name, CONCAT('Connection ', connection_type)) AS connection_type_name,

    -- Geographic properties (denormalized with lookups)
    geo_country,
    COALESCE(country_lookup.name, CONCAT('Country ', geo_country)) AS country_name,
    geo_region,
    geo_city,

    -- Traffic source properties
    post_referrer AS referrer_url,
    ref_domain,
    ref_type,
    COALESCE(referrer_type_lookup.name, CONCAT('Referrer Type ', ref_type)) AS referrer_type_name,
    va_closer_detail,
    va_finder_detail,

    -- Business-friendly eVar names from Silver layer
    evar_page_name,
    evar_current_url,
    evar_time_since_last_use,
    evar_primary_category,
    evar_visitor_id,
    evar_form_page_url,
    evar_visit_duration,
    evar_conductor_export_variable,
    evar_location,

    -- Business-friendly prop names from Silver layer
    prop_page_name,
    prop_current_url,
    prop_previous_page,
    prop_new_vs_repeat_visitors,
    CAST(NULL AS STRING) AS primary_category_prop,
    CAST(NULL AS STRING) AS visitor_id_prop,
    CAST(NULL AS STRING) AS form_page_url_prop,
    CAST(NULL AS STRING) AS location_prop,
    CAST(NULL AS STRING) AS tag_name,

    -- Timing
    visit_page_num,

    0 AS event_sequence,  -- Page view is always first

    -- Metadata
    ts_utc AS original_timestamp,
    'page_view' AS hit_type,
    original_hit_no

  FROM hit_base
  LEFT JOIN `${project}.${dataset}.lookup_browser` browser_lookup ON SAFE_CAST(browser AS INT64) = browser_lookup.id
  LEFT JOIN `${project}.${dataset}.lookup_operating_systems` os_lookup ON SAFE_CAST(os AS INT64) = os_lookup.id
  LEFT JOIN `${project}.${dataset}.lookup_connection_type` connection_lookup ON SAFE_CAST(connection_type AS INT64) = connection_lookup.id
  LEFT JOIN `${project}.${dataset}.lookup_country` country_lookup ON SAFE_CAST(geo_country AS INT64) = country_lookup.id
  LEFT JOIN `${project}.${dataset}.lookup_referrer_type` referrer_type_lookup ON SAFE_CAST(ref_type AS INT64) = referrer_type_lookup.id
  WHERE is_page_view

  UNION ALL

  -- Business events (one per event in business_events array)
  SELECT
    TO_HEX(MD5(CONCAT(distinct_id, '-', COALESCE(event_map.name, event.event_name), '-', CAST(UNIX_MICROS(TIMESTAMP_ADD(ts_utc, INTERVAL (event_index + 1) * 5 SECOND)) AS STRING)))) AS insert_id,
    TIMESTAMP_ADD(ts_utc, INTERVAL (event_index + 1) * 5 SECOND) AS ts_utc,  -- Nudge timestamps
    distinct_id,
    COALESCE(event_map.name, event.event_name) AS event_name,
    event.event_code AS original_event_code,
    event.event_name AS original_event_name,
    FALSE AS is_page_view,
    NOT is_page_view AS is_link_tracking,

    -- Page context
    page_url,
    pagename AS page_name,
    COALESCE(pagename, page_url) AS page_title,

    -- Measurement properties (inherited from hit)
    measurements.page_load_time,
    measurements.time_on_page,
    measurements.scroll_depth,
    measurements.form_starts,
    measurements.form_completions,
    measurements.video_plays,
    measurements.downloads,
    measurements.external_links,
    measurements.custom_link_clicks,

    -- Dynamic measurement columns
    measurements.measurement_209_page_load_time,
    measurements.measurement_236_page_scroll_75,
    measurements.measurement_240,

    -- Event-specific value
    event.event_value,

    -- Visitor properties
    distinct_id AS visitor_id,
    visit_num,
    CASE
      WHEN SAFE_CAST(visit_num AS INT64) = 1 THEN 'New'
      ELSE 'Returning'
    END AS visitor_type,
    yearly_visitor,
    monthly_visitor,
    daily_visitor,
    hourly_visitor,

    -- Technology properties
    browser,
    COALESCE(browser_lookup.name, CONCAT('Browser ', browser)) AS browser_name,
    os,
    COALESCE(os_lookup.name, CONCAT('OS ', os)) AS operating_system_name,
    user_agent,
    c_color AS color_depth,
    javascript AS javascript_version,
    java_enabled,
    connection_type,
    COALESCE(connection_lookup.name, CONCAT('Connection ', connection_type)) AS connection_type_name,

    -- Geographic properties
    geo_country,
    COALESCE(country_lookup.name, CONCAT('Country ', geo_country)) AS country_name,
    geo_region,
    geo_city,

    -- Traffic source properties
    post_referrer AS referrer_url,
    ref_domain,
    ref_type,
    COALESCE(referrer_type_lookup.name, CONCAT('Referrer Type ', ref_type)) AS referrer_type_name,
    va_closer_detail,
    va_finder_detail,

    -- Business-friendly names
    evar_page_name,
    evar_current_url,
    evar_time_since_last_use,
    evar_primary_category,
    evar_visitor_id,
    evar_form_page_url,
    evar_visit_duration,
    evar_conductor_export_variable,
    evar_location,

    prop_page_name,
    prop_current_url,
    prop_previous_page,
    prop_new_vs_repeat_visitors,
    CAST(NULL AS STRING) AS primary_category_prop,
    CAST(NULL AS STRING) AS visitor_id_prop,
    CAST(NULL AS STRING) AS form_page_url_prop,
    CAST(NULL AS STRING) AS location_prop,
    CAST(NULL AS STRING) AS tag_name,

    -- Timing
    visit_page_num,

    event_index + 1 AS event_sequence,

    -- Metadata
    ts_utc AS original_timestamp,
    CASE
      WHEN is_page_view THEN 'event_on_page'
      ELSE 'link_tracking'
    END AS hit_type,
    original_hit_no

  FROM hit_base,
  UNNEST(business_events) AS event WITH OFFSET AS event_index
  LEFT JOIN `${project}.${dataset}.event_map` event_map ON SAFE_CAST(event.event_code AS INT64) = event_map.code
  LEFT JOIN `${project}.${dataset}.lookup_browser` browser_lookup ON SAFE_CAST(browser AS INT64) = browser_lookup.id
  LEFT JOIN `${project}.${dataset}.lookup_operating_systems` os_lookup ON SAFE_CAST(os AS INT64) = os_lookup.id
  LEFT JOIN `${project}.${dataset}.lookup_connection_type` connection_lookup ON SAFE_CAST(connection_type AS INT64) = connection_lookup.id
  LEFT JOIN `${project}.${dataset}.lookup_country` country_lookup ON SAFE_CAST(geo_country AS INT64) = country_lookup.id
  LEFT JOIN `${project}.${dataset}.lookup_referrer_type` referrer_type_lookup ON SAFE_CAST(ref_type AS INT64) = referrer_type_lookup.id

  UNION ALL

  -- Fallback events for hits with no page view and no business events but have measurements
  SELECT
    TO_HEX(MD5(CONCAT(distinct_id, '-', 'Action Tracked', '-', CAST(UNIX_MICROS(ts_utc) AS STRING)))) AS insert_id,
    ts_utc,
    distinct_id,
    'Action Tracked' AS event_name,
    NULL AS original_event_code,
    NULL AS original_event_name,
    FALSE AS is_page_view,
    TRUE AS is_link_tracking,

    -- Page context
    page_url,
    pagename AS page_name,
    COALESCE(pagename, page_url, 'Unknown') AS page_title,

    -- Measurements are the main content for fallback events
    measurements.page_load_time,
    measurements.time_on_page,
    measurements.scroll_depth,
    measurements.form_starts,
    measurements.form_completions,
    measurements.video_plays,
    measurements.downloads,
    measurements.external_links,
    measurements.custom_link_clicks,

    -- Dynamic measurement columns
    measurements.measurement_209_page_load_time,
    measurements.measurement_236_page_scroll_75,
    measurements.measurement_240,

    CAST(NULL AS FLOAT64) AS event_value,

    -- Visitor properties
    distinct_id AS visitor_id,
    visit_num,
    CASE
      WHEN SAFE_CAST(visit_num AS INT64) = 1 THEN 'New'
      ELSE 'Returning'
    END AS visitor_type,
    yearly_visitor,
    monthly_visitor,
    daily_visitor,
    hourly_visitor,

    -- Technology properties
    browser,
    COALESCE(browser_lookup.name, CONCAT('Browser ', browser)) AS browser_name,
    os,
    COALESCE(os_lookup.name, CONCAT('OS ', os)) AS operating_system_name,
    user_agent,
    c_color AS color_depth,
    javascript AS javascript_version,
    java_enabled,
    connection_type,
    COALESCE(connection_lookup.name, CONCAT('Connection ', connection_type)) AS connection_type_name,

    -- Geographic properties
    geo_country,
    COALESCE(country_lookup.name, CONCAT('Country ', geo_country)) AS country_name,
    geo_region,
    geo_city,

    -- Traffic source properties
    post_referrer AS referrer_url,
    ref_domain,
    ref_type,
    COALESCE(referrer_type_lookup.name, CONCAT('Referrer Type ', ref_type)) AS referrer_type_name,
    va_closer_detail,
    va_finder_detail,

    -- Business-friendly names
    evar_page_name,
    evar_current_url,
    evar_time_since_last_use,
    evar_primary_category,
    evar_visitor_id,
    evar_form_page_url,
    evar_visit_duration,
    evar_conductor_export_variable,
    evar_location,

    prop_page_name,
    prop_current_url,
    prop_previous_page,
    prop_new_vs_repeat_visitors,
    CAST(NULL AS STRING) AS primary_category_prop,
    CAST(NULL AS STRING) AS visitor_id_prop,
    CAST(NULL AS STRING) AS form_page_url_prop,
    CAST(NULL AS STRING) AS location_prop,
    CAST(NULL AS STRING) AS tag_name,

    -- Timing
    visit_page_num,

    999 AS event_sequence,  -- Fallback events are last

    -- Metadata
    ts_utc AS original_timestamp,
    'measurement_only' AS hit_type,
    original_hit_no

  FROM hit_base
  LEFT JOIN `${project}.${dataset}.lookup_browser` browser_lookup ON SAFE_CAST(browser AS INT64) = browser_lookup.id
  LEFT JOIN `${project}.${dataset}.lookup_operating_systems` os_lookup ON SAFE_CAST(os AS INT64) = os_lookup.id
  LEFT JOIN `${project}.${dataset}.lookup_connection_type` connection_lookup ON SAFE_CAST(connection_type AS INT64) = connection_lookup.id
  LEFT JOIN `${project}.${dataset}.lookup_country` country_lookup ON SAFE_CAST(geo_country AS INT64) = country_lookup.id
  LEFT JOIN `${project}.${dataset}.lookup_referrer_type` referrer_type_lookup ON SAFE_CAST(ref_type AS INT64) = referrer_type_lookup.id
  WHERE NOT is_page_view
    AND ARRAY_LENGTH(business_events) = 0
    AND (
      measurements.page_load_time IS NOT NULL
      OR measurements.time_on_page IS NOT NULL
      OR measurements.scroll_depth IS NOT NULL
      OR measurements.form_starts IS NOT NULL
      OR measurements.form_completions IS NOT NULL
      OR measurements.video_plays IS NOT NULL
      OR measurements.downloads IS NOT NULL
      OR measurements.external_links IS NOT NULL
      OR measurements.custom_link_clicks IS NOT NULL
    )
)

SELECT * FROM events_exploded
WHERE ts_utc IS NOT NULL;