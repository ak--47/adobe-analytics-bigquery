#!/usr/bin/env node

import { readFile } from 'fs/promises';
import { Logger, BigQueryHelper } from './utils.js';
import { transformToBronze } from './3-transform-bronze.js';
import { transformToSilver } from './3-transform-silver.js';
import { transformToGold } from './3-transform-gold.js';

export async function transform(config) {
  Logger.info('=== Transformation Phase (Bronze → Silver → Gold) ===\n\n');
  console.log();

  try {
    // Step 1: Raw → Bronze (parsing & cleaning)
    Logger.info('🥉 Step 1: Transform raw to bronze (parsing & cleaning)');
    await transformToBronze(config);
    Logger.success('✅ Bronze transformation complete\n');

    // Step 2: Bronze → Silver (SDR mappings)
    Logger.info('🥈 Step 2: Transform bronze to silver (SDR mappings)');
    await transformToSilver(config);
    Logger.success('✅ Silver transformation complete\n');

    // Step 3: Silver → Gold (eventification)
    Logger.info('🥇 Step 3: Transform silver to gold (eventification)');
    await transformToGold(config);
    Logger.success('✅ Gold transformation complete\n');

    // Generate comprehensive transformation summary
    await generateTransformationSummary(config);

    Logger.success('🎉 All transformation phases completed successfully!');
  } catch (error) {
    Logger.error(`Transformation phase failed: ${error.message}`);
    throw error;
  }
}

async function generateTransformationSummary(config) {
  Logger.info('=== Transformation Summary ===\n\n');

  const bq = new BigQueryHelper(config);

  // Tables to analyze
  const transformationTables = [
    { name: config.tables.raw, type: 'Raw', description: 'Original Adobe Analytics data', layer: 0 },
    { name: config.tables.bronze, type: 'Bronze', description: 'Parsed and cleaned data', layer: 1 },
    { name: config.tables.silver, type: 'Silver', description: 'SDR-mapped business data', layer: 2 },
    { name: config.tables.gold, type: 'Gold', description: 'Eventified analytics data', layer: 3 }
  ];

  const layerStats = [];

  // Get statistics for each layer
  for (const table of transformationTables) {
    try {
      if (await bq.tableExists(table.name)) {
        let statsSql;

        if (table.type === 'Raw') {
          // Raw table doesn't have ts_utc or distinct_id yet
          statsSql = `
            SELECT
              COUNT(*) as total_rows,
              COUNT(DISTINCT CONCAT(CAST(post_visid_high AS STRING), '-', CAST(post_visid_low AS STRING))) as unique_visitors,
              0 as date_range_days,
              CAST(NULL AS TIMESTAMP) as earliest_timestamp,
              CAST(NULL AS TIMESTAMP) as latest_timestamp
            FROM \`${config.project}.${config.dataset}.${table.name}\`
          `;
        } else {
          // Bronze, Silver, Gold have ts_utc and distinct_id
          statsSql = `
            SELECT
              COUNT(*) as total_rows,
              COUNT(DISTINCT distinct_id) as unique_visitors,
              COUNT(DISTINCT DATE(ts_utc)) as date_range_days,
              MIN(ts_utc) as earliest_timestamp,
              MAX(ts_utc) as latest_timestamp
            FROM \`${config.project}.${config.dataset}.${table.name}\`
            WHERE ts_utc IS NOT NULL
          `;
        }

        const [rows] = await bq.bq.query(statsSql);
        const stats = rows[0];

        layerStats.push({
          ...table,
          ...stats,
          total_rows: parseInt(stats.total_rows),
          unique_visitors: parseInt(stats.unique_visitors),
          date_range_days: parseInt(stats.date_range_days)
        });
      } else {
        layerStats.push({
          ...table,
          total_rows: 0,
          unique_visitors: 0,
          date_range_days: 0,
          missing: true
        });
      }
    } catch (error) {
      layerStats.push({
        ...table,
        error: error.message
      });
    }
  }

  // Display layer progression
  Logger.info('📊 Data Flow Through Pipeline Layers:');
  console.log();

  layerStats.forEach((layer, index) => {
    if (layer.missing) {
      Logger.warn(`  ${layer.type}: Table not found (${layer.name})`);
    } else if (layer.error) {
      Logger.error(`  ${layer.type}: Error - ${layer.error}`);
    } else {
      const rowsFormatted = layer.total_rows.toLocaleString();
      const visitorsFormatted = layer.unique_visitors.toLocaleString();

      Logger.success(`  ${layer.type}: ${rowsFormatted} rows, ${visitorsFormatted} visitors - ${layer.description}`);

      // Show expansion/reduction from previous layer
      if (index > 0 && layerStats[index - 1] && !layerStats[index - 1].missing && !layerStats[index - 1].error) {
        const prevLayer = layerStats[index - 1];
        const rowRatio = layer.total_rows / prevLayer.total_rows;
        const visitorRatio = layer.unique_visitors / prevLayer.unique_visitors;

        if (rowRatio > 1.1) {
          Logger.info(`    ↗️  ${rowRatio.toFixed(2)}x row expansion (eventification)`);
        } else if (rowRatio < 0.9) {
          Logger.info(`    ↘️  ${(1/rowRatio).toFixed(2)}x row reduction (optimization)`);
        } else {
          Logger.info(`    ➡️  Similar row count (${rowRatio.toFixed(2)}x)`);
        }

        if (visitorRatio !== 1.0) {
          Logger.warn(`    Visitor count changed: ${visitorRatio.toFixed(3)}x`);
        } else {
          Logger.success(`    Visitor count preserved`);
        }
      }
    }
    console.log();
  });

  // Overall pipeline statistics
  const rawLayer = layerStats.find(l => l.type === 'Raw');
  const goldLayer = layerStats.find(l => l.type === 'Gold');

  if (rawLayer && goldLayer && !rawLayer.missing && !goldLayer.missing && !rawLayer.error && !goldLayer.error) {
    Logger.info('📈 Overall Pipeline Transformation:');

    const overallExpansion = goldLayer.total_rows / rawLayer.total_rows;
    Logger.info(`  • Data expansion: ${overallExpansion.toFixed(2)}x (${rawLayer.total_rows.toLocaleString()} hits → ${goldLayer.total_rows.toLocaleString()} events)`);

    if (rawLayer.unique_visitors === goldLayer.unique_visitors) {
      Logger.info(`  • Visitor preservation: ${goldLayer.unique_visitors.toLocaleString()} visitors maintained`);
    } else {
      Logger.info(`  • Visitor count change: ${rawLayer.unique_visitors.toLocaleString()} → ${goldLayer.unique_visitors.toLocaleString()}`);
    }

    if (goldLayer.date_range_days > 0) {
      Logger.info(`  • Date range: ${goldLayer.date_range_days} days`);

      // Format timestamps properly - handle BigQuery timestamp format
      let startDate, endDate;
      try {
        // BigQuery returns timestamps as objects, need to extract the value
        const startTimestamp = goldLayer.earliest_timestamp?.value || goldLayer.earliest_timestamp;
        const endTimestamp = goldLayer.latest_timestamp?.value || goldLayer.latest_timestamp;

        startDate = new Date(startTimestamp).toISOString().split('T')[0];
        endDate = new Date(endTimestamp).toISOString().split('T')[0];
        Logger.info(`• Time span: ${startDate} to ${endDate}`);
      } catch (error) {
        Logger.info(`• Time span: Unable to parse timestamps (${error.message})`);
      }
    }

    // Get event-specific stats for Gold layer
    try {
      const eventStatsSql = `
        SELECT
          COUNT(CASE WHEN event = 'Page Viewed' THEN 1 END) as page_views,
          COUNT(CASE WHEN event != 'Page Viewed' THEN 1 END) as business_events,
          COUNT(DISTINCT event) as unique_event_types
        FROM \`${config.project}.${config.dataset}.${goldLayer.name}\`
      `;

      const [eventRows] = await bq.bq.query(eventStatsSql);
      const eventStats = eventRows[0];

      Logger.info(`• Event breakdown: ${parseInt(eventStats.page_views).toLocaleString()} page views, ${parseInt(eventStats.business_events).toLocaleString()} business events`);
      Logger.info(`• Event diversity: ${eventStats.unique_event_types} unique event types`);

    } catch (error) {
      Logger.warn(`  • Could not get event statistics: ${error.message}`);
    }
  }

  console.log();
}

// Allow running as standalone script
if (import.meta.url === `file://${process.argv[1]}`) {
  try {
    const configData = await readFile('./config.json', 'utf8');
    const config = JSON.parse(configData);
    await transform(config);
  } catch (error) {
    Logger.error(`Transformation failed: ${error.message}`);
    process.exit(1);
  }
}