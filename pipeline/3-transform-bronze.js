#!/usr/bin/env node

import { readFile } from 'fs/promises';
import { Logger, BigQueryHelper, loadSqlTemplate } from './utils.js';

async function performBronzeDataQuality(config, bq, stats) {
  Logger.info('Running Bronze data quality checks...');

  const dqSql = `
    WITH total AS (SELECT ${stats.total_rows} as total_rows),
    null_ts AS (
      SELECT COUNT(*) as count
      FROM \`${config.project}.${config.dataset}.${config.tables.bronze}\`
      WHERE ts_utc IS NULL
    ),
    empty_id AS (
      SELECT COUNT(*) as count
      FROM \`${config.project}.${config.dataset}.${config.tables.bronze}\`
      WHERE COALESCE(distinct_id, '') = ''
    ),
    duplicate_hits AS (
      SELECT COUNT(*) - COUNT(DISTINCT CONCAT(hitid_high, '-', hitid_low)) as count
      FROM \`${config.project}.${config.dataset}.${config.tables.bronze}\`
      WHERE hitid_high IS NOT NULL AND hitid_low IS NOT NULL
    )
    SELECT
      total.total_rows,
      SAFE_DIVIDE(null_ts.count, total.total_rows) * 100 as pct_null_timestamp,
      SAFE_DIVIDE(empty_id.count, total.total_rows) * 100 as pct_empty_visitor_id,
      duplicate_hits.count as duplicate_hit_count
    FROM total, null_ts, empty_id, duplicate_hits
  `;

  const [dqRows] = await bq.bq.query(dqSql);
  const dq = dqRows[0];

  // Check thresholds and warn
  const thresholds = config.pipeline_config.data_quality;

  if (dq.pct_null_timestamp > thresholds.null_timestamp_warn_pct * 100) {
    Logger.warn(`⚠️  High null timestamp rate: ${dq.pct_null_timestamp.toFixed(2)}% (threshold: ${thresholds.null_timestamp_warn_pct * 100}%)`);
  } else {
    Logger.success(`✅ Timestamp quality: ${(100 - dq.pct_null_timestamp).toFixed(1)}% valid timestamps`);
  }

  if (dq.pct_empty_visitor_id > thresholds.empty_visitor_id_warn_pct * 100) {
    Logger.warn(`⚠️  High empty visitor ID rate: ${dq.pct_empty_visitor_id.toFixed(2)}% (threshold: ${thresholds.empty_visitor_id_warn_pct * 100}%)`);
  } else {
    Logger.success(`✅ Visitor ID quality: ${(100 - dq.pct_empty_visitor_id).toFixed(1)}% valid visitor IDs`);
  }

  if (dq.duplicate_hit_count > 0) {
    Logger.warn(`⚠️  Found ${dq.duplicate_hit_count} duplicate hit IDs`);
  } else {
    Logger.success(`✅ No duplicate hit IDs detected`);
  }

  // Validation against expected rows
  if (config.validation.expectedRows > 0) {
    const rowDiff = Math.abs(stats.total_rows - config.validation.expectedRows);
    const rowDiffPct = (rowDiff / config.validation.expectedRows) * 100;

    if (rowDiffPct > 10) {
      Logger.warn(`⚠️  Row count validation: Expected ~${config.validation.expectedRows}, got ${stats.total_rows} (${rowDiffPct.toFixed(1)}% difference)`);
    } else {
      Logger.success(`✅ Row count validation passed: ${stats.total_rows} rows`);
    }
  }
}

export async function transformToBronze(config) {
  Logger.info('=== Bronze Transformation Phase ===\n\n');
  Logger.info(`Transforming ${config.tables.raw} → ${config.tables.bronze}`);
  console.log();

  const bq = new BigQueryHelper(config);

  // Ensure dataset exists
  await bq.ensureDataset();

  // Check if Bronze table already exists
  if (await bq.tableExists(config.tables.bronze)) {
    Logger.info(`Bronze table ${config.tables.bronze} already exists; skipping creation.`);
    return;
  }

  Logger.info(`Creating Bronze table: ${config.tables.bronze}`);

  // First, generate dynamic column cleaning SQL using INFORMATION_SCHEMA
  Logger.info('Generating dynamic column cleaning SQL...');
  const columnGenSql = await loadSqlTemplate('./models/generate-bronze-columns.sql', {
    project: config.project,
    dataset: config.dataset,
    rawTable: config.tables.raw
  });

  const [columnRows] = await bq.bq.query(columnGenSql);
  const cleanedColumns = columnRows[0].cleaned_columns;

  // Load Bronze SQL template with dynamic columns
  const bronzeSql = await loadSqlTemplate('./models/create-bronze-clean.sql', {
    project: config.project,
    dataset: config.dataset,
    rawTable: config.tables.raw,
    bronzeTable: config.tables.bronze,
    cleanedColumns: cleanedColumns
  });

  await bq.executeQuery(bronzeSql);

  // Get row count
  const countSql = `SELECT COUNT(*) as row_count FROM \`${config.project}.${config.dataset}.${config.tables.bronze}\``;
  const [rows] = await bq.bq.query(countSql);
  Logger.success(`Bronze table created with ${rows[0].row_count} rows`);

  // Quality check - validate distinct_id and timestamp parsing
  const qualitySql = `
    SELECT
      COUNT(*) as total_rows,
      COUNT(DISTINCT distinct_id) as unique_visitors,
      COUNT(ts_utc) as valid_timestamps,
      COUNT(events_array) as rows_with_events,
      SUM(ARRAY_LENGTH(events_array)) as total_events
    FROM \`${config.project}.${config.dataset}.${config.tables.bronze}\`
  `;

  const [qualityRows] = await bq.bq.query(qualitySql);
  const stats = qualityRows[0];

  Logger.info('Bronze table quality metrics:');
  Logger.info(`  • Total rows: ${stats.total_rows}`);
  Logger.info(`  • Unique visitors: ${stats.unique_visitors}`);
  Logger.info(`  • Valid timestamps: ${stats.valid_timestamps}`);
  Logger.info(`  • Rows with events: ${stats.rows_with_events}`);
  Logger.info(`  • Total events: ${stats.total_events}`);

  // Enhanced data quality checks
  await performBronzeDataQuality(config, bq, stats);

  Logger.success('Bronze transformation complete');
}

// Allow running as standalone script
if (import.meta.url === `file://${process.argv[1]}`) {
  try {
    const configData = await readFile('./config.json', 'utf8');
    const config = JSON.parse(configData);
    await transformToBronze(config);
  } catch (error) {
    Logger.error(`Bronze transformation failed: ${error.message}`);
    process.exit(1);
  }
}