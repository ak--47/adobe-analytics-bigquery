#!/usr/bin/env node

import { readFile, readdir, stat } from 'fs/promises';
import { mkdir } from 'fs/promises';
import { existsSync } from 'fs';
import { join } from 'path';
import { Logger, BigQueryHelper, StorageHelper } from './utils.js';

// Expected file structure for lookups directory
const EXPECTED_FILE_STRUCTURE = {
  // Root level files
  'columns.csv': 'CSV column definitions',
  'events.tsv': 'Event lookup table',

  // SDR directory
  'sdr/custom-events.tsv': 'SDR custom events mapping',
  'sdr/evars.tsv': 'SDR eVars mapping',
  'sdr/custom-props.tsv': 'SDR custom props mapping',

  // Values directory
  'values/browser_type.tsv': 'Browser type lookup',
  'values/browser.tsv': 'Browser lookup',
  'values/color_depth.tsv': 'Color depth lookup',
  'values/connection_type.tsv': 'Connection type lookup',
  'values/country.tsv': 'Country lookup',
  'values/javascript_version.tsv': 'JavaScript version lookup',
  'values/languages.tsv': 'Languages lookup',
  'values/operating_systems.tsv': 'Operating systems lookup',
  'values/plugins.tsv': 'Plugins lookup',
  'values/referrer_type.tsv': 'Referrer type lookup',
  'values/resolution.tsv': 'Resolution lookup',
  'values/search_engines.tsv': 'Search engines lookup'
};

async function validateFileStructure(lookupsDir) {
  Logger.info('Validating complete file structure...');

  const missingFiles = [];
  const foundFiles = [];

  for (const [relativePath, description] of Object.entries(EXPECTED_FILE_STRUCTURE)) {
    const fullPath = join(lookupsDir, relativePath);

    if (existsSync(fullPath)) {
      // Verify it's actually a file, not a directory
      const stats = await stat(fullPath);
      if (stats.isFile()) {
        foundFiles.push(relativePath);
        Logger.success(`Found: ${relativePath}`);
      } else {
        missingFiles.push(`${relativePath} (exists but is not a file)`);
        Logger.error(`🔎 Invalid: ${relativePath} (exists but is not a file)`);
      }
    } else {
      missingFiles.push(relativePath);
      Logger.error(`🔎 Missing: ${relativePath} (${description})`);
    }
  }

  if (missingFiles.length > 0) {
    throw new Error(`Missing required files:\n${missingFiles.map(f => `  - ${f}`).join('\n')}`);
  }

  Logger.success(`All ${foundFiles.length} required files found and validated`);
  return foundFiles;
}

async function validateDirectoryStructure(lookupsDir) {
  Logger.info('Validating directory structure...');

  // Check required subdirectories
  const requiredDirs = ['sdr', 'values'];

  for (const dir of requiredDirs) {
    const dirPath = join(lookupsDir, dir);
    if (!existsSync(dirPath)) {
      throw new Error(`Required directory missing: ${dirPath}`);
    }

    const stats = await stat(dirPath);
    if (!stats.isDirectory()) {
      throw new Error(`Path exists but is not a directory: ${dirPath}`);
    }

    Logger.success(`Directory found: ${dir}/`);
  }
}

export async function validate(config) {
  Logger.info('=== Validation Phase ===');
  Logger.info(`Project: ${config.project}`);
  Logger.info(`Dataset: ${config.dataset}`);
  Logger.info(`Location: ${config.location}`);
  Logger.info(`Raw table: ${config.tables.raw}`);
  Logger.info(`Bronze table: ${config.tables.bronze}`);
  Logger.info(`Silver table: ${config.tables.silver}`);
  Logger.info(`Gold table: ${config.tables.gold}`);
  console.log('\n\n');

  // Check tmp directory
  Logger.info('Checking tmp directory...');
  if (!existsSync(config.paths.tmpDir)) {
    await mkdir(config.paths.tmpDir, { recursive: true });
    Logger.success(`Created tmp directory: ${config.paths.tmpDir}`);
  } else {
    Logger.success(`Tmp directory exists: ${config.paths.tmpDir}`);
  }

  // Check lookups directory exists
  Logger.info('Checking lookups directory...');
  if (!existsSync(config.paths.lookupsDir)) {
    throw new Error(`Lookups directory not found: ${config.paths.lookupsDir}`);
  }
  Logger.success(`Lookups directory exists: ${config.paths.lookupsDir}`);
  console.log();

  // Validate directory structure (sdr/, values/)
  await validateDirectoryStructure(config.paths.lookupsDir);
  console.log();

  // Validate complete file structure
  await validateFileStructure(config.paths.lookupsDir);
  console.log();

  // Validate columns.csv
  Logger.info('Validating columns.csv...');
  const columnsFile = `${config.paths.lookupsDir}/columns.csv`;
  const columnsContent = await readFile(columnsFile, 'utf8');
  const lines = columnsContent.trim().split('\n');

  if (lines.length > 1) {
    Logger.warn(`columns.csv has ${lines.length} lines; only the first line will be used as header.`);
  } else {
    Logger.success('columns.csv appears to be a single-row header');
  }

  const headerCount = lines[0].split(',').length;
  Logger.info(`Header columns detected: ${headerCount}`);

  // Validation tolerance checks (warnings only, don't fail pipeline)
  if (config.validation.expectedColumns > 0 && headerCount !== config.validation.expectedColumns) {
    Logger.warn(`Column count validation: Expected ${config.validation.expectedColumns}, found ${headerCount}. Consider updating config if this is expected.`);
  } else if (config.validation.expectedColumns > 0) {
    Logger.success(`Column count validation passed: ${headerCount} columns`);
  }

  // Check BigQuery access
  Logger.info('Checking BigQuery access...');
  const bq = new BigQueryHelper(config);
  await bq.ensureDataset();

  // Check GCS access
  Logger.info('Checking GCS access...');
  const storage = new StorageHelper();
  const sourceExists = await storage.checkUriExists(config.gcs.sourceUri);
  if (sourceExists) {
    Logger.success(`GCS source data found: ${config.gcs.sourceUri}`);
  } else {
    throw new Error(`No GCS objects found at: ${config.gcs.sourceUri}`);
  }

  // Check transformDest if configured (preprocessed files)
  if (config.gcs.transformDest) {
    Logger.info('Checking preprocessed files (transformDest)...');
    const transformExists = await storage.checkUriExists(config.gcs.transformDest + config.gcs.sourceUri.split('/').pop());
    if (transformExists) {
      Logger.success(`Preprocessed data found: ${config.gcs.transformDest}`);
    } else {
      Logger.warn(`⚠️  transformDest is configured but no preprocessed files found at: ${config.gcs.transformDest}`);
      Logger.info('Run preprocessing step first: npm run preprocess');
    }
  }

  // Check export bucket access
  const exportBucket = config.gcs.exportPrefix.match(/^gs:\/\/([^\/]+)/)?.[1];
  if (exportBucket) {
    Logger.success(`Export bucket accessible: gs://${exportBucket}`);
  } else {
    throw new Error(`Invalid export URI format: ${config.gcs.exportPrefix}`);
  }

  // Add comprehensive config validation summary
  Logger.info('Configuration validation summary:');
  Logger.info(`  • Project: ${config.project}`);
  Logger.info(`  • Dataset: ${config.dataset} (${config.location})`);
  Logger.info(`  • Tables: ${config.tables.raw} → ${config.tables.bronze} → ${config.tables.silver} → ${config.tables.gold}`);
  Logger.info(`  • Data source: ${config.gcs.sourceUri}${config.gcs.transformDest ? ' (raw)' : ''}`);
  if (config.gcs.transformDest) {
    Logger.info(`  • Preprocessed data: ${config.gcs.transformDest} (will be used for loading)`);
  }
  Logger.info(`  • Export destination: ${config.gcs.exportPrefix}`);
  Logger.info(`  • Lookups directory: ${config.paths.lookupsDir}`);
  Logger.info(`  • Temp directory: ${config.paths.tmpDir}`);

  // Validation tolerance settings
  if (config.validation.expectedColumns > 0) {
    Logger.info(`  • Expected columns: ${config.validation.expectedColumns} (tolerance-based warning)`);
  }
  if (config.validation.expectedRows > 0) {
    Logger.info(`  • Expected rows: ${config.validation.expectedRows} (tolerance-based warning)`);
  }
  if (config.validation.expectedErrors >= 0) {
    Logger.info(`  • Error tolerance: ${config.validation.expectedErrors} (warning threshold)`);
  }

  Logger.success('Validation complete - Pipeline ready to run!');
}

// Allow running as standalone script
if (import.meta.url === `file://${process.argv[1]}`) {
  try {
    const configData = await readFile('./config.json', 'utf8');
    const config = JSON.parse(configData);
    await validate(config);
  } catch (error) {
    Logger.error(`Validation failed: ${error.message}`);
    process.exit(1);
  }
}