-- Transform raw Adobe Analytics data to Bronze layer
-- Handles parsing, cleaning, and basic data quality fixes
CREATE OR REPLACE TABLE `${project}.${dataset}.${bronzeTable}` AS
SELECT
  -- Basic metadata (keep all original columns except the ones we're transforming)
  * EXCEPT (
    post_cust_hit_time_gmt,
    post_visid_high,
    post_visid_low,
    mcvisid,
    post_event_list
  ),

  -- Parse timestamp with fallback hierarchy: post_cust_hit_time_gmt → cust_hit_time_gmt → hit_time_gmt
  CASE
    -- Try post_cust_hit_time_gmt first
    WHEN LENGTH(CAST(post_cust_hit_time_gmt AS STRING)) = 10 AND SAFE_CAST(post_cust_hit_time_gmt AS INT64) IS NOT NULL THEN
      TIMESTAMP_SECONDS(SAFE_CAST(post_cust_hit_time_gmt AS INT64))
    WHEN LENGTH(CAST(post_cust_hit_time_gmt AS STRING)) = 13 AND SAFE_CAST(post_cust_hit_time_gmt AS INT64) IS NOT NULL THEN
      TIMESTAMP_MILLIS(SAFE_CAST(post_cust_hit_time_gmt AS INT64))
    -- Try cust_hit_time_gmt second
    WHEN LENGTH(CAST(cust_hit_time_gmt AS STRING)) = 10 AND SAFE_CAST(cust_hit_time_gmt AS INT64) IS NOT NULL THEN
      TIMESTAMP_SECONDS(SAFE_CAST(cust_hit_time_gmt AS INT64))
    WHEN LENGTH(CAST(cust_hit_time_gmt AS STRING)) = 13 AND SAFE_CAST(cust_hit_time_gmt AS INT64) IS NOT NULL THEN
      TIMESTAMP_MILLIS(SAFE_CAST(cust_hit_time_gmt AS INT64))
    -- Try hit_time_gmt last
    WHEN LENGTH(CAST(hit_time_gmt AS STRING)) = 10 AND SAFE_CAST(hit_time_gmt AS INT64) IS NOT NULL THEN
      TIMESTAMP_SECONDS(SAFE_CAST(hit_time_gmt AS INT64))
    WHEN LENGTH(CAST(hit_time_gmt AS STRING)) = 13 AND SAFE_CAST(hit_time_gmt AS INT64) IS NOT NULL THEN
      TIMESTAMP_MILLIS(SAFE_CAST(hit_time_gmt AS INT64))
    ELSE NULL
  END AS ts_utc,

  -- Create distinct_id with precedence: post_visid → mcvisid → post_cust_visid → fallback
  COALESCE(
    CASE
      WHEN post_visid_high IS NOT NULL AND post_visid_low IS NOT NULL
      THEN CONCAT(CAST(post_visid_high AS STRING), '-', CAST(post_visid_low AS STRING))
      ELSE NULL
    END,
    NULLIF(NULLIF(NULLIF(mcvisid, ''), ':'), '-'),
    NULLIF(NULLIF(NULLIF(post_cust_visid, ''), ':'), '-'),
    CONCAT('fallback-', CAST(ROW_NUMBER() OVER (ORDER BY post_cust_hit_time_gmt) AS STRING))
  ) AS distinct_id,

  -- Parse event list into structured array (prefer post_event_list over event_list)
  CASE
    WHEN COALESCE(NULLIF(TRIM(post_event_list), ''), NULLIF(TRIM(event_list), '')) IS NULL THEN []
    ELSE ARRAY(
      SELECT AS STRUCT
        CASE
          WHEN STRPOS(event_item, '=') > 0 THEN SUBSTR(event_item, 1, STRPOS(event_item, '=') - 1)
          ELSE event_item
        END AS event_code,
        CASE
          WHEN STRPOS(event_item, '=') > 0 THEN SAFE_CAST(SUBSTR(event_item, STRPOS(event_item, '=') + 1) AS FLOAT64)
          ELSE NULL
        END AS event_value
      FROM UNNEST(SPLIT(COALESCE(NULLIF(TRIM(post_event_list), ''), NULLIF(TRIM(event_list), '')), ',')) AS event_item
      WHERE event_item IS NOT NULL AND event_item != ''
    )
  END AS events_array,

  -- Store the cleaned event list for reference (prefer post_ over raw)
  COALESCE(
    NULLIF(NULLIF(NULLIF(NULLIF(post_event_list, ''), ':'), '-'), '--'),
    NULLIF(NULLIF(NULLIF(NULLIF(event_list, ''), ':'), '-'), '--')
  ) AS events_list_clean

FROM `${project}.${dataset}.${rawTable}`
WHERE NOT (post_cust_hit_time_gmt IS NULL
       AND cust_hit_time_gmt IS NULL
       AND hit_time_gmt IS NULL);