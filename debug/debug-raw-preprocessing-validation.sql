-- Debug Raw Layer Preprocessing Validation
-- Validates that preprocessing correctly handled malformed timestamp and hit ID fields


-- SUMMARY STATISTICS: Count malformed records
SELECT
  COUNT(*) AS total_records,

  -- Count malformed hit_time_gmt (NULL or not exactly 10 digits)
  COUNT(CASE WHEN hit_time_gmt IS NULL OR NOT REGEXP_CONTAINS(hit_time_gmt, r'^[0-9]{10}$') THEN 1 END) AS malformed_hit_time_gmt,

  -- Count malformed hitid_high (NULL or not 10-20 digits)
  COUNT(CASE WHEN hitid_high IS NULL OR NOT REGEXP_CONTAINS(hitid_high, r'^[0-9]{10,20}$') THEN 1 END) AS malformed_hitid_high,

  -- Count malformed hitid_low (NULL or not 10-20 digits)
  COUNT(CASE WHEN hitid_low IS NULL OR NOT REGEXP_CONTAINS(hitid_low, r'^[0-9]{10,20}$') THEN 1 END) AS malformed_hitid_low,

  -- Count records with ANY malformed critical field (will be dropped in Bronze)
  COUNT(CASE WHEN
    (hit_time_gmt IS NULL OR NOT REGEXP_CONTAINS(hit_time_gmt, r'^[0-9]{10}$'))
    OR (hitid_high IS NULL OR NOT REGEXP_CONTAINS(hitid_high, r'^[0-9]{10,20}$'))
    OR (hitid_low IS NULL OR NOT REGEXP_CONTAINS(hitid_low, r'^[0-9]{10,20}$'))
  THEN 1 END) AS total_malformed_records,

  -- Calculate percentages
  ROUND(COUNT(CASE WHEN hit_time_gmt IS NULL OR NOT REGEXP_CONTAINS(hit_time_gmt, r'^[0-9]{10}$') THEN 1 END) / COUNT(*) * 100, 4) AS malformed_hit_time_gmt_pct,
  ROUND(COUNT(CASE WHEN hitid_high IS NULL OR NOT REGEXP_CONTAINS(hitid_high, r'^[0-9]{10,20}$') THEN 1 END) / COUNT(*) * 100, 4) AS malformed_hitid_high_pct,
  ROUND(COUNT(CASE WHEN hitid_low IS NULL OR NOT REGEXP_CONTAINS(hitid_low, r'^[0-9]{10,20}$') THEN 1 END) / COUNT(*) * 100, 4) AS malformed_hitid_low_pct,
  ROUND(COUNT(CASE WHEN
    (hit_time_gmt IS NULL OR NOT REGEXP_CONTAINS(hit_time_gmt, r'^[0-9]{10}$'))
    OR (hitid_high IS NULL OR NOT REGEXP_CONTAINS(hitid_high, r'^[0-9]{10,20}$'))
    OR (hitid_low IS NULL OR NOT REGEXP_CONTAINS(hitid_low, r'^[0-9]{10,20}$'))
  THEN 1 END) / COUNT(*) * 100, 4) AS total_malformed_pct,

  -- Clean records that will survive to Bronze
  COUNT(*) - COUNT(CASE WHEN
    (hit_time_gmt IS NULL OR NOT REGEXP_CONTAINS(hit_time_gmt, r'^[0-9]{10}$'))
    OR (hitid_high IS NULL OR NOT REGEXP_CONTAINS(hitid_high, r'^[0-9]{10,20}$'))
    OR (hitid_low IS NULL OR NOT REGEXP_CONTAINS(hitid_low, r'^[0-9]{10,20}$'))
  THEN 1 END) AS clean_records_for_bronze

FROM `mixpanel-gtm-training.korn_ferry_adobe_main.kfmain_raw`;

-- BAD RECORDS: Select all malformed records for inspection
SELECT
  hit_time_gmt,
  hitid_high,
  hitid_low,
  LENGTH(hit_time_gmt) AS hit_time_length,
  LENGTH(hitid_high) AS hitid_high_length,
  LENGTH(hitid_low) AS hitid_low_length,

  -- Identify the specific issue(s)
  CASE
    WHEN hit_time_gmt IS NULL THEN 'NULL_timestamp'
    WHEN NOT REGEXP_CONTAINS(hit_time_gmt, r'^[0-9]{10}$') THEN 'bad_timestamp_format'
    ELSE 'good_timestamp'
  END AS timestamp_issue,

  CASE
    WHEN hitid_high IS NULL THEN 'NULL_hitid_high'
    WHEN NOT REGEXP_CONTAINS(hitid_high, r'^[0-9]{10,20}$') THEN 'bad_hitid_high_format'
    ELSE 'good_hitid_high'
  END AS hitid_high_issue,

  CASE
    WHEN hitid_low IS NULL THEN 'NULL_hitid_low'
    WHEN NOT REGEXP_CONTAINS(hitid_low, r'^[0-9]{10,20}$') THEN 'bad_hitid_low_format'
    ELSE 'good_hitid_low'
  END AS hitid_low_issue,

  -- Additional debugging columns (first few and last few columns)
  accept_language,
  pagename,
  page_url

FROM `mixpanel-gtm-training.korn_ferry_adobe_main.kfmain_raw`
WHERE
  -- Show only malformed records
  (hit_time_gmt IS NULL OR NOT REGEXP_CONTAINS(hit_time_gmt, r'^[0-9]{10}$'))
  OR (hitid_high IS NULL OR NOT REGEXP_CONTAINS(hitid_high, r'^[0-9]{10,20}$'))
  OR (hitid_low IS NULL OR NOT REGEXP_CONTAINS(hitid_low, r'^[0-9]{10,20}$'))

ORDER BY
  -- Show worst cases first (multiple issues)
  CASE WHEN hit_time_gmt IS NULL OR NOT REGEXP_CONTAINS(hit_time_gmt, r'^[0-9]{10}$') THEN 1 ELSE 0 END +
  CASE WHEN hitid_high IS NULL OR NOT REGEXP_CONTAINS(hitid_high, r'^[0-9]{10,20}$') THEN 1 ELSE 0 END +
  CASE WHEN hitid_low IS NULL OR NOT REGEXP_CONTAINS(hitid_low, r'^[0-9]{10,20}$') THEN 1 ELSE 0 END DESC

LIMIT 100; -- Limit to first 100 bad records for inspection

-- FIELD LENGTH DISTRIBUTION: Understanding the data patterns
SELECT 'hit_time_gmt_length_distribution' AS analysis_type,
       LENGTH(hit_time_gmt) AS field_length,
       COUNT(*) AS record_count,
       ROUND(COUNT(*) / (SELECT COUNT(*) FROM `mixpanel-gtm-training.korn_ferry_adobe_main.kfmain_raw`) * 100, 2) AS percentage
FROM `mixpanel-gtm-training.korn_ferry_adobe_main.kfmain_raw`
WHERE hit_time_gmt IS NOT NULL
GROUP BY LENGTH(hit_time_gmt)

UNION ALL

SELECT 'hitid_high_length_distribution' AS analysis_type,
       LENGTH(hitid_high) AS field_length,
       COUNT(*) AS record_count,
       ROUND(COUNT(*) / (SELECT COUNT(*) FROM `mixpanel-gtm-training.korn_ferry_adobe_main.kfmain_raw`) * 100, 2) AS percentage
FROM `mixpanel-gtm-training.korn_ferry_adobe_main.kfmain_raw`
WHERE hitid_high IS NOT NULL
GROUP BY LENGTH(hitid_high)

UNION ALL

SELECT 'hitid_low_length_distribution' AS analysis_type,
       LENGTH(hitid_low) AS field_length,
       COUNT(*) AS record_count,
       ROUND(COUNT(*) / (SELECT COUNT(*) FROM `mixpanel-gtm-training.korn_ferry_adobe_main.kfmain_raw`) * 100, 2) AS percentage
FROM `mixpanel-gtm-training.korn_ferry_adobe_main.kfmain_raw`
WHERE hitid_low IS NOT NULL
GROUP BY LENGTH(hitid_low)

ORDER BY analysis_type, field_length;