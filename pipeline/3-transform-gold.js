#!/usr/bin/env node

import { readFile } from 'fs/promises';
import { Logger, BigQueryHelper, loadSqlTemplate } from './utils.js';

export async function transformToGold(config) {
  Logger.info('=== Gold Transformation Phase ===\n\n');
  Logger.info(`Transforming ${config.tables.silver} → ${config.tables.gold} (Eventification)`);
  console.log();

  const bq = new BigQueryHelper(config);

  // Ensure dataset exists
  await bq.ensureDataset();

  // Check if Gold table already exists
  if (await bq.tableExists(config.tables.gold)) {
    Logger.info(`Gold table ${config.tables.gold} already exists; skipping creation.`);
    return;
  }

  // Ensure SDR mapping tables exist and are up-to-date
  Logger.info('Verifying SDR mapping tables...');
  const requiredTables = ['sdr_custom_events', 'sdr_evars', 'event_map'];
  const missingTables = [];

  for (const tableName of requiredTables) {
    if (!(await bq.tableExists(tableName))) {
      missingTables.push(tableName);
    }
  }

  if (missingTables.length > 0) {
    Logger.info(`Missing SDR tables: ${missingTables.join(', ')}. Creating them now...`);
    const sdrSql = await loadSqlTemplate('./models/create-sdr-maps.sql', {
      project: config.project,
      dataset: config.dataset
    });
    await bq.executeQuery(sdrSql);
    Logger.success('SDR mapping tables created');
  } else {
    Logger.success('SDR mapping tables verified');
  }

  Logger.info(`Creating Gold table: ${config.tables.gold}`);

  // Load SQL template and execute
  const goldSql = await loadSqlTemplate('./models/create-gold.sql', {
    project: config.project,
    dataset: config.dataset,
    silverTable: config.tables.silver,
    goldTable: config.tables.gold,
    measurementEventCodes: JSON.stringify(config.pipeline_config.measurement_event_codes),
    ignoreHits: JSON.stringify(config.pipeline_config.ignore_hits)
  });

  await bq.executeQuery(goldSql);

  // Get comprehensive metrics
  const metricsSql = `
    SELECT
      COUNT(*) as total_events,
      COUNT(DISTINCT distinct_id) as unique_visitors,
      COUNT(DISTINCT insert_id) as unique_events,
      SUM(CASE WHEN event_name = 'Page Viewed' THEN 1 ELSE 0 END) as page_view_events,
      SUM(CASE WHEN is_link_tracking THEN 1 ELSE 0 END) as link_tracking_events,
      COUNT(DISTINCT event_name) as unique_event_types,
      COUNT(DISTINCT DATE(ts_utc)) as date_range_days,
      ROUND(AVG(SAFE_CAST(visit_page_num AS FLOAT64)), 2) as avg_pages_per_visit,
      ROUND(AVG(SAFE_CAST(event_sequence AS FLOAT64)), 2) as avg_events_per_hit
    FROM \`${config.project}.${config.dataset}.${config.tables.gold}\`
  `;

  const [metricsRows] = await bq.bq.query(metricsSql);
  const metrics = metricsRows[0];

  Logger.success(`Gold table created with ${metrics.total_events} events`);

  Logger.info('Gold table eventification metrics:');
  Logger.info(`  • Total events: ${metrics.total_events}`);
  Logger.info(`  • Unique visitors: ${metrics.unique_visitors}`);
  Logger.info(`  • Page view events: ${metrics.page_view_events}`);
  Logger.info(`  • Link tracking events: ${metrics.link_tracking_events}`);
  Logger.info(`  • Unique event types: ${metrics.unique_event_types}`);
  Logger.info(`  • Date range: ${metrics.date_range_days} days`);
  Logger.info(`  • Avg pages per visit: ${metrics.avg_pages_per_visit}`);
  Logger.info(`  • Avg events per hit: ${metrics.avg_events_per_hit}`);

  // Get Silver row count for DQ comparison
  const silverCountSql = `SELECT COUNT(*) as row_count FROM \`${config.project}.${config.dataset}.${config.tables.silver}\``;
  const [silverRows] = await bq.bq.query(silverCountSql);

  // Sample the most common events
  const topEventsSql = `
    SELECT event_name, COUNT(*) as event_count
    FROM \`${config.project}.${config.dataset}.${config.tables.gold}\`
    GROUP BY event_name
    ORDER BY event_count DESC
    LIMIT 10
  `;

  const [topEventsRows] = await bq.bq.query(topEventsSql);
  Logger.info('Top 10 event types:');
  topEventsRows.forEach((row, i) => {
    Logger.info(`  ${i + 1}. ${row.event_name}: ${row.event_count} events`);
  });

  // Gold-specific data quality checks
  await performGoldDataQuality(config, bq, { ...stats, silver_rows: silverRows[0].row_count });

  Logger.success('Gold transformation complete - data is now fully eventified!');
}

async function performGoldDataQuality(config, bq, stats) {
  Logger.info('Running Gold layer data quality checks...');

  const goldTable = config.tables.gold;

  // Check for event distribution issues
  const eventDistSql = `
    SELECT
      COUNT(CASE WHEN event_name = 'Page Viewed' THEN 1 END) as page_views,
      COUNT(CASE WHEN event_name != 'Page Viewed' THEN 1 END) as business_events,
      COUNT(DISTINCT event_name) as unique_event_types,
      COUNT(DISTINCT distinct_id) as unique_visitors
    FROM \`${config.project}.${config.dataset}.${goldTable}\`
  `;

  const [eventRows] = await bq.bq.query(eventDistSql);
  const eventStats = eventRows[0];

  // Validate eventification worked correctly
  const expansionRatio = stats.total_events / stats.silver_rows;
  Logger.info(`Event expansion ratio: ${expansionRatio.toFixed(2)}x (${stats.total_events} events from ${stats.silver_rows} hits)`);

  if (expansionRatio < 1.0) {
    Logger.warn('Event expansion ratio < 1.0 - possible data loss during eventification');
  } else if (expansionRatio > 10.0) {
    Logger.warn('Event expansion ratio > 10.0 - possible over-eventification');
  } else {
    Logger.success(`Event expansion ratio looks healthy (${expansionRatio.toFixed(2)}x)`);
  }

  // Validate visitor preservation
  if (eventStats.unique_visitors === stats.unique_visitors) {
    Logger.success(`Visitor preservation validated (${eventStats.unique_visitors} visitors)`);
  } else {
    Logger.warn(`Visitor count mismatch: Gold=${eventStats.unique_visitors}, expected=${stats.unique_visitors}`);
  }

  // Check event type distribution
  Logger.info(`Event type distribution:`);
  Logger.info(`  • Page Views: ${eventStats.page_views.toLocaleString()}`);
  Logger.info(`  • Business Events: ${eventStats.business_events.toLocaleString()}`);
  Logger.info(`  • Unique Event Types: ${eventStats.unique_event_types}`);

  // Warn if no page views
  if (eventStats.page_views === 0) {
    Logger.warn('No page views found - check page view detection logic');
  }

  // Warn if too few event types
  if (eventStats.unique_event_types < 2) {
    Logger.warn('Very few unique event types found - check event mapping');
  }

  Logger.success('Gold data quality checks complete');
}

// Allow running as standalone script
if (import.meta.url === `file://${process.argv[1]}`) {
  try {
    const configData = await readFile('./config.json', 'utf8');
    const config = JSON.parse(configData);
    await transformToGold(config);
  } catch (error) {
    Logger.error(`Gold transformation failed: ${error.message}`);
    process.exit(1);
  }
}