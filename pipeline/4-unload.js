#!/usr/bin/env node

import { readFile } from 'fs/promises';
import { Logger, BigQueryHelper } from './utils.js';

export async function unload(config) {
  Logger.info('=== Export Phase ===\n\n');
  Logger.info(`Exporting ${config.project}.${config.dataset}.${config.tables.gold} -> ${config.gcs.exportPrefix}`);
  console.log();

  const bq = new BigQueryHelper(config);

  // Export all data without date filtering

  // Export query
  const exportSql = `
    EXPORT DATA OPTIONS(
      uri='${config.gcs.exportPrefix}',
      format='JSON',
      compression='GZIP'
    )
    AS
    SELECT
      ts_utc,
      distinct_id,
      is_page_view,
      hits_found,
      events,
      page_url,
      pagename,
      geo_country,
      country_name,         -- denormalized label if mapped
      browser_name,
      operating_system_name,
      user_agent
    FROM \`${config.project}.${config.dataset}.${config.tables.gold}\`
  `;

  Logger.info('Starting export job...');
  await bq.executeQuery(exportSql);

  Logger.success('Export complete');
  Logger.info(`Data exported to: ${config.gcs.exportPrefix}`);
}

// Allow running as standalone script
if (import.meta.url === `file://${process.argv[1]}`) {
  try {
    const configData = await readFile('./config.json', 'utf8');
    const config = JSON.parse(configData);
    await unload(config);
  } catch (error) {
    Logger.error(`Export failed: ${error.message}`);
    process.exit(1);
  }
}