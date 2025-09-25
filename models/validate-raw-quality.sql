-- Raw Data Quality Validation
-- Checks for malformed timestamps and hit IDs that will be dropped in Bronze transformation
-- Should be run immediately after loading raw table to quantify data quality issues

WITH quality_check AS (
  SELECT
    COUNT(*) AS total_records,

    -- Count malformed hit_time_gmt (NULL or not exactly 10 digits)
    COUNTIF(hit_time_gmt IS NULL OR NOT REGEXP_CONTAINS(hit_time_gmt, r'^[0-9]{10}$')) AS malformed_hit_time_gmt,

    -- Count malformed hitid_high (NULL or not exactly 19 digits)
    COUNTIF(hitid_high IS NULL OR NOT REGEXP_CONTAINS(hitid_high, r'^[0-9]{19}$')) AS malformed_hitid_high,

    -- Count malformed hitid_low (NULL or not exactly 19 digits)
    COUNTIF(hitid_low IS NULL OR NOT REGEXP_CONTAINS(hitid_low, r'^[0-9]{19}$')) AS malformed_hitid_low,

    -- Count records with ANY malformed field (will be dropped in Bronze)
    COUNTIF(
      (hit_time_gmt IS NULL OR NOT REGEXP_CONTAINS(hit_time_gmt, r'^[0-9]{10}$'))
      OR (hitid_high IS NULL OR NOT REGEXP_CONTAINS(hitid_high, r'^[0-9]{19}$'))
      OR (hitid_low IS NULL OR NOT REGEXP_CONTAINS(hitid_low, r'^[0-9]{19}$'))
    ) AS malformed_record_count

  FROM `${project}`.`${dataset}`.`${rawTable}`
)

SELECT
  total_records,
  malformed_hit_time_gmt,
  malformed_hitid_high,
  malformed_hitid_low,
  malformed_record_count,

  -- Calculate percentages
  ROUND(malformed_hit_time_gmt / total_records * 100, 3) AS malformed_hit_time_gmt_pct,
  ROUND(malformed_hitid_high / total_records * 100, 3) AS malformed_hitid_high_pct,
  ROUND(malformed_hitid_low / total_records * 100, 3) AS malformed_hitid_low_pct,
  ROUND(malformed_record_count / total_records * 100, 3) AS malformed_record_pct,

  -- Calculate clean records that will make it to Bronze
  (total_records - malformed_record_count) AS clean_records,
  ROUND((total_records - malformed_record_count) / total_records * 100, 3) AS clean_record_pct

FROM quality_check;