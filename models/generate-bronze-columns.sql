-- Generate dynamic column cleaning SQL for all STRING columns
-- This implements the INFORMATION_SCHEMA approach mentioned in Adobe best practices
SELECT
  STRING_AGG(
    CASE
      WHEN data_type = 'STRING' AND column_name NOT IN ('post_cust_hit_time_gmt', 'post_visid_high', 'post_visid_low', 'mcvisid', 'post_event_list', 'event_list') THEN
        CONCAT('NULLIF(NULLIF(NULLIF(NULLIF(', column_name, ', ""), ":"), "-"), "--") AS ', column_name)
      WHEN column_name NOT IN ('post_cust_hit_time_gmt', 'post_visid_high', 'post_visid_low', 'mcvisid', 'post_event_list', 'event_list') THEN
        column_name
      ELSE NULL
    END,
    ',\n  '
    ORDER BY ordinal_position
  ) AS cleaned_columns
FROM `${project}.${dataset}.INFORMATION_SCHEMA.COLUMNS`
WHERE table_name = '${rawTable}'
  AND column_name NOT IN ('post_cust_hit_time_gmt', 'post_visid_high', 'post_visid_low', 'mcvisid', 'post_event_list', 'event_list');