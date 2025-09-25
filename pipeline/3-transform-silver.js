#!/usr/bin/env node

import { readFile } from 'fs/promises';
import { Logger, BigQueryHelper, loadSqlTemplate } from './utils.js';

async function generateSDRColumnAliases(config, bq) {
  Logger.info('Generating dynamic SDR-based column aliases...');

  // Query eVars mappings from SDR (only standard eVar range 100-199 = eVar1-100)
  const evarsSql = `
    SELECT code, name_override
    FROM \`${config.project}.${config.dataset}.sdr_evars\`
    WHERE name_override IS NOT NULL
      AND name_override != ''
      AND code BETWEEN 100 AND 199
    ORDER BY code
  `;
  const [evarsRows] = await bq.bq.query(evarsSql);

  // Query props mappings from SDR
  const propsSql = `
    SELECT PropertyNumber, Name
    FROM \`${config.project}.${config.dataset}.sdr_props_raw\`
    WHERE Name IS NOT NULL AND Name != ''
    ORDER BY PropertyNumber
  `;
  const [propsRows] = await bq.bq.query(propsSql);

  // Generate eVar aliases (prefix with evar_ to avoid conflicts)
  const evarAliases = evarsRows.map(row => {
    const evarNum = row.code - 99; // Convert code back to eVar number (code 100 = eVar1)
    const cleanName = row.name_override.toLowerCase().replace(/[^a-zA-Z0-9]/g, '_');
    return `    post_evar${evarNum} AS evar_${cleanName},`;
  }).join('\n');

  // Generate prop aliases (prefix with prop_ to avoid conflicts)
  const propAliases = propsRows.map(row => {
    const cleanName = row.Name.toLowerCase().replace(/[^a-zA-Z0-9]/g, '_');
    return `    prop${row.PropertyNumber} AS prop_${cleanName},`;
  }).join('\n');

  return {
    evarAliases,
    propAliases
  };
}

async function performSilverDataQuality(config, bq, stats) {
  Logger.info('Running Silver data quality checks...');

  const dqSql = `
    WITH unknown_events AS (
      SELECT COUNT(*) as count
      FROM \`${config.project}.${config.dataset}.${config.tables.silver}\`,
      UNNEST(events_enhanced) AS event
      LEFT JOIN \`${config.project}.${config.dataset}.event_map\` em ON event.event_code = em.code
      WHERE em.name IS NULL
    ),
    total_events AS (
      SELECT SUM(ARRAY_LENGTH(events_enhanced)) as count
      FROM \`${config.project}.${config.dataset}.${config.tables.silver}\`
    ),
    page_view_distribution AS (
      SELECT
        COUNTIF(is_page_view) as page_views,
        COUNTIF(NOT is_page_view) as non_page_views,
        COUNT(*) as total_hits
      FROM \`${config.project}.${config.dataset}.${config.tables.silver}\`
    )
    SELECT
      unknown_events.count as unknown_event_count,
      total_events.count as total_event_count,
      SAFE_DIVIDE(unknown_events.count, total_events.count) * 100 as pct_unknown_events,
      page_view_distribution.*,
      SAFE_DIVIDE(page_views, total_hits) * 100 as pct_page_views
    FROM unknown_events, total_events, page_view_distribution
  `;

  const [dqRows] = await bq.bq.query(dqSql);
  const dq = dqRows[0];

  const thresholds = config.pipeline_config.data_quality;

  // Check unknown events
  if (dq.pct_unknown_events > thresholds.unknown_events_warn_pct * 100) {
    Logger.warn(`⚠️  High unknown event rate: ${dq.pct_unknown_events.toFixed(2)}% (${dq.unknown_event_count}/${dq.total_event_count})`);
    Logger.warn(`   Consider updating your SDR files or event_map table`);
  } else {
    Logger.success(`✅ Event mapping quality: ${(100 - dq.pct_unknown_events).toFixed(1)}% events have names`);
  }

  // Check page view distribution (sanity check)
  if (dq.pct_page_views < 10) {
    Logger.warn(`⚠️  Low page view rate: ${dq.pct_page_views.toFixed(1)}% (${dq.page_views}/${dq.total_hits})`);
    Logger.warn(`   Check page view detection configuration`);
  } else if (dq.pct_page_views > 90) {
    Logger.warn(`⚠️  Very high page view rate: ${dq.pct_page_views.toFixed(1)}% - might be over-detecting`);
  } else {
    Logger.success(`✅ Page view distribution: ${dq.pct_page_views.toFixed(1)}% page views, ${(100-dq.pct_page_views).toFixed(1)}% interactions`);
  }
}

export async function transformToSilver(config) {
  Logger.info('=== Silver Transformation Phase ===\n\n');
  Logger.info(`Transforming ${config.tables.bronze} → ${config.tables.silver} (SDR mappings)`);
  console.log();

  const bq = new BigQueryHelper(config);

  // Ensure dataset exists
  await bq.ensureDataset();

  // Check if Silver table already exists
  if (await bq.tableExists(config.tables.silver)) {
    Logger.info(`Silver table ${config.tables.silver} already exists; skipping creation.`);
    return;
  }

  // First create the event mapping table (combines base events + SDR overrides)
  Logger.info('Creating event mapping table with SDR overrides...');
  const eventMapSql = await loadSqlTemplate('./models/create-event-map.sql', {
    project: config.project,
    dataset: config.dataset
  });
  await bq.executeQuery(eventMapSql);
  Logger.success('Event mapping table created');

  Logger.info(`Creating Silver table: ${config.tables.silver}`);

  // Generate dynamic SDR-based column aliases
  const sdrAliases = await generateSDRColumnAliases(config, bq);

  // Load SQL template with pipeline config parameters and dynamic SDR aliases
  const pipelineConfig = config.pipeline_config;
  const silverSql = await loadSqlTemplate('./models/create-silver.sql', {
    project: config.project,
    dataset: config.dataset,
    bronzeTable: config.tables.bronze,
    silverTable: config.tables.silver,
    usePostPageEvent: pipelineConfig.page_view_detection.use_post_page_event,
    useEvarInstances: pipelineConfig.page_view_detection.use_evar_instances,
    useUrlChange: pipelineConfig.page_view_detection.use_url_change,
    measurementEventCodes: JSON.stringify(pipelineConfig.measurement_event_codes),
    evarAliases: sdrAliases.evarAliases,
    propAliases: sdrAliases.propAliases
  });

  await bq.executeQuery(silverSql);

  // Get row count and validation
  const countSql = `SELECT COUNT(*) as row_count FROM \`${config.project}.${config.dataset}.${config.tables.silver}\``;
  const [rows] = await bq.bq.query(countSql);
  Logger.success(`Silver table created with ${rows[0].row_count} rows`);

  // Quality metrics for Silver
  const qualitySql = `
    SELECT
      COUNT(*) as total_rows,
      COUNT(DISTINCT distinct_id) as unique_visitors,
      SUM(CASE WHEN is_page_view THEN 1 ELSE 0 END) as page_views,
      SUM(CASE WHEN NOT is_page_view THEN 1 ELSE 0 END) as link_tracking_hits,
      SUM(ARRAY_LENGTH(events_enhanced)) as total_events,
      COUNT(DISTINCT DATE(ts_utc)) as unique_days
    FROM \`${config.project}.${config.dataset}.${config.tables.silver}\`
  `;

  const [qualityRows] = await bq.bq.query(qualitySql);
  const stats = qualityRows[0];

  Logger.info('Silver table quality metrics:');
  Logger.info(`  • Total rows: ${stats.total_rows}`);
  Logger.info(`  • Unique visitors: ${stats.unique_visitors}`);
  Logger.info(`  • Page views: ${stats.page_views}`);
  Logger.info(`  • Link tracking hits: ${stats.link_tracking_hits}`);
  Logger.info(`  • Total events: ${stats.total_events}`);
  Logger.info(`  • Date range: ${stats.unique_days} unique days`);

  // Silver-specific data quality checks
  await performSilverDataQuality(config, bq, stats);

  Logger.success('Silver transformation complete');
}

// Allow running as standalone script
if (import.meta.url === `file://${process.argv[1]}`) {
  try {
    const configData = await readFile('./config.json', 'utf8');
    const config = JSON.parse(configData);
    await transformToSilver(config);
  } catch (error) {
    Logger.error(`Silver transformation failed: ${error.message}`);
    process.exit(1);
  }
}