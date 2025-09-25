#!/usr/bin/env node

import { readFile } from 'fs/promises';
import { Logger, BigQueryHelper } from './utils.js';

async function listAllTablesAndViews(bq) {
  try {
    const [tables] = await bq.dataset.getTables();
    const tableNames = tables.map(table => table.id);

    if (tableNames.length === 0) {
      Logger.info('No tables or views found in dataset');
      return [];
    }

    Logger.info(`Found ${tableNames.length} tables/views in dataset:`);
    tableNames.forEach(name => Logger.info(`  • ${name}`));

    return tableNames;
  } catch (error) {
    if (error.code === 404) {
      Logger.info('Dataset does not exist - nothing to reset');
      return [];
    }
    throw error;
  }
}

async function deleteTable(bq, tableName) {
  try {
    const table = bq.dataset.table(tableName);
    await table.delete();
    Logger.info(`Deleted: ${tableName}`);
  } catch (error) {
    if (error.code === 404) {
      Logger.warn(`Table ${tableName} not found (already deleted?)`);
    } else {
      Logger.error(`Failed to delete ${tableName}: ${error.message}`);
      throw error;
    }
  }
}

export async function reset(config) {
  Logger.info('=== Reset Phase ===\n\n');
  Logger.info(`Project: ${config.project}`);
  Logger.info(`Dataset: ${config.dataset}`);
  console.log();

  const bq = new BigQueryHelper(config);

  // List all tables and views in the dataset
  const tableNames = await listAllTablesAndViews(bq);

  if (tableNames.length === 0) {
    Logger.success('Dataset is already clean - nothing to reset');
    return;
  }

  Logger.warn(`⚠️  This will DELETE ${tableNames.length} tables/views from dataset: ${config.dataset}`);
  Logger.info('Proceeding with deletion in 2 seconds...');

  // Small delay to let user see the warning
  await new Promise(resolve => setTimeout(resolve, 2000));

  // Delete all tables and views
  let deletedCount = 0;
  for (const tableName of tableNames) {
    try {
      await deleteTable(bq, tableName);
      deletedCount++;
    } catch (error) {
      Logger.error(`Failed to delete ${tableName}, continuing with remaining tables...`);
    }
  }

  if (deletedCount === tableNames.length) {
    Logger.success(`Successfully deleted all ${deletedCount} tables/views from dataset`);
  } else {
    Logger.warn(`Deleted ${deletedCount} out of ${tableNames.length} tables/views - some failures occurred`);
  }

  Logger.success('Reset phase complete - dataset is now clean');
}

// Allow running as standalone script
if (import.meta.url === `file://${process.argv[1]}`) {
  try {
    const configData = await readFile('./config.json', 'utf8');
    const config = JSON.parse(configData);
    await reset(config);
  } catch (error) {
    Logger.error(`Reset failed: ${error.message}`);
    process.exit(1);
  }
}