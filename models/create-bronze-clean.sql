-- Generate dynamic NULL cleaning for all STRING columns as mentioned in Adobe best practices
-- This creates the actual Bronze transformation with proper NULL handling
CREATE OR REPLACE TABLE `${project}.${dataset}.${bronzeTable}` AS
SELECT
  -- Dynamically clean all STRING columns by replacing '', ':', '-', '--' with NULL
  ${cleanedColumns},

  -- Robust timestamp parsing with multiple fallbacks and regex validation
  CASE
    -- Try post_cust_hit_time_gmt first (13-digit milliseconds)
    WHEN REGEXP_CONTAINS(CAST(post_cust_hit_time_gmt AS STRING), r'^\d{13}$') THEN
      TIMESTAMP_MILLIS(CAST(post_cust_hit_time_gmt AS INT64))
    -- Try post_cust_hit_time_gmt (10-digit seconds)
    WHEN REGEXP_CONTAINS(CAST(post_cust_hit_time_gmt AS STRING), r'^\d{10}$') THEN
      TIMESTAMP_SECONDS(CAST(post_cust_hit_time_gmt AS INT64))
    -- Try cust_hit_time_gmt second (13-digit milliseconds)
    WHEN REGEXP_CONTAINS(CAST(cust_hit_time_gmt AS STRING), r'^\d{13}$') THEN
      TIMESTAMP_MILLIS(CAST(cust_hit_time_gmt AS INT64))
    -- Try cust_hit_time_gmt (10-digit seconds)
    WHEN REGEXP_CONTAINS(CAST(cust_hit_time_gmt AS STRING), r'^\d{10}$') THEN
      TIMESTAMP_SECONDS(CAST(cust_hit_time_gmt AS INT64))
    -- Try hit_time_gmt last (13-digit milliseconds)
    WHEN REGEXP_CONTAINS(CAST(hit_time_gmt AS STRING), r'^\d{13}$') THEN
      TIMESTAMP_MILLIS(CAST(hit_time_gmt AS INT64))
    -- Try hit_time_gmt (10-digit seconds)
    WHEN REGEXP_CONTAINS(CAST(hit_time_gmt AS STRING), r'^\d{10}$') THEN
      TIMESTAMP_SECONDS(CAST(hit_time_gmt AS INT64))
    -- Last resort: parse date_time if available
    WHEN SAFE.PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S', date_time) IS NOT NULL THEN
      SAFE.PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S', date_time)
    ELSE NULL
  END AS ts_utc,

  -- Adobe visitor identity with proper precedence and robust fallback
  CASE
    WHEN post_visid_high IS NOT NULL AND post_visid_high != ''
     AND post_visid_low IS NOT NULL AND post_visid_low != ''
      THEN CONCAT(post_visid_high, '-', post_visid_low)
    WHEN mcvisid IS NOT NULL AND mcvisid != '' AND mcvisid NOT IN (':', '-', '--')
      THEN mcvisid
    WHEN post_cust_visid IS NOT NULL AND post_cust_visid != '' AND post_cust_visid NOT IN (':', '-', '--')
      THEN CONCAT('cust:', post_cust_visid)
    ELSE CONCAT('fp:', TO_HEX(MD5(CONCAT(
      COALESCE(ip, ''), '|',
      COALESCE(user_agent, ''), '|',
      COALESCE(accept_language, '')
    ))))
  END AS distinct_id,

  -- Robust event parsing with regex-based code/value extraction
  CASE
    WHEN COALESCE(NULLIF(TRIM(post_event_list), ''), NULLIF(TRIM(event_list), '')) IS NULL THEN []
    ELSE ARRAY(
      SELECT AS STRUCT
        CAST(REGEXP_EXTRACT(tok, r'^(\d+)') AS INT64) AS event_code,
        SAFE_CAST(REGEXP_EXTRACT(tok, r'=\s*([-+]?\d*\.?\d+)') AS FLOAT64) AS event_value
      FROM UNNEST(SPLIT(REGEXP_REPLACE(
        COALESCE(NULLIF(TRIM(post_event_list), ''), NULLIF(TRIM(event_list), '')),
        r'\s+', ''
      ), ',')) AS tok
      WHERE tok IS NOT NULL
        AND tok != ''
        AND REGEXP_CONTAINS(tok, r'^\d+')
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