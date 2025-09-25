#!/usr/bin/env node

import { readFile, writeFile, rm, mkdir, readdir } from 'fs/promises';
import { existsSync } from 'fs';
import { join } from 'path';
import { Logger } from './utils.js';

function sanitizeColumnName(name) {
  // Trim whitespace and remove quotes
  let sanitized = name.trim().replace(/^["']|["']$/g, '');

  // Convert to lowercase and replace non-alphanumeric with underscore
  sanitized = sanitized.toLowerCase().replace(/[^a-z0-9_]/g, '_');

  // Replace multiple underscores with single
  sanitized = sanitized.replace(/_+/g, '_');

  // Remove leading/trailing underscores
  sanitized = sanitized.replace(/^_+|_+$/g, '');

  // Add prefix if starts with number
  if (/^[0-9]/.test(sanitized)) {
    sanitized = `f_${sanitized}`;
  }

  // Default if empty
  if (!sanitized) {
    sanitized = 'col';
  }

  return sanitized;
}


export async function prepare(config) {
  Logger.info('=== Schema Preparation Phase ===\n\n');

  // Clear tmp directory while preserving .gitkeep
  Logger.info('Cleaning temporary directory...');
  if (existsSync(config.paths.tmpDir)) {
    const files = await readdir(config.paths.tmpDir);
    const filesToRemove = files.filter(file => file !== '.gitkeep');

    if (filesToRemove.length > 0) {
      await Promise.all(
        filesToRemove.map(file =>
          rm(join(config.paths.tmpDir, file), { recursive: true, force: true })
        )
      );
      Logger.success(`Cleared ${filesToRemove.length} items from tmp directory (preserved .gitkeep)`);
    } else {
      Logger.success('Tmp directory already clean');
    }
  } else {
    await mkdir(config.paths.tmpDir, { recursive: true });
    Logger.success(`Created tmp directory: ${config.paths.tmpDir}`);
  }
  console.log();

  Logger.info(`Input header: ${config.paths.lookupsDir}/columns.csv`);
  Logger.info(`Schema output: ${config.paths.tmpDir}/schema.json`);
  Logger.info(`Mapping output: ${config.paths.tmpDir}/sanitize-map.tsv`);
  console.log();

  // Read and parse CSV header
  const columnsFile = `${config.paths.lookupsDir}/columns.csv`;
  const csvContent = await readFile(columnsFile, 'utf8');
  const headerRow = csvContent.trim().split('\n')[0];
  const rawColumns = headerRow.split(',');

  Logger.info('Processing column names...');

  // Keep all columns for Raw/Bronze/Silver layers - optimization happens only in Gold
  Logger.info(`Processing ${rawColumns.length} columns for Raw/Bronze/Silver layers`);
  Logger.info('Note: Column optimization (removing non-post columns) will be applied only in Gold layer');

  const mappings = [];
  const schema = [];
  const usedNames = new Map();

  rawColumns.forEach((rawColumn, index) => {
    let sanitized = sanitizeColumnName(rawColumn);

    // Handle duplicates by adding suffix
    const count = usedNames.get(sanitized) || 0;
    usedNames.set(sanitized, count + 1);

    if (count > 0) {
      sanitized = `${sanitized}_${count + 1}`;
    }

    // Store mapping
    mappings.push({
      position: index + 1,
      original: rawColumn,
      sanitized: sanitized
    });

    // Add to schema
    schema.push({
      name: sanitized,
      type: 'STRING',
      mode: 'NULLABLE'
    });
  });

  // Write schema JSON
  const schemaJson = `${config.paths.tmpDir}/schema.json`;
  await writeFile(schemaJson, JSON.stringify(schema, null, 2));
  Logger.success(`Schema written: ${schemaJson}`);

  // Write mapping TSV
  const mappingTsv = mappings
    .map(m => `${m.position}\t${m.original}\t${m.sanitized}`)
    .join('\n');

  const sanitizeMap = `${config.paths.tmpDir}/sanitize-map.tsv`;
  await writeFile(sanitizeMap, mappingTsv);
  Logger.success(`Mapping written: ${sanitizeMap}`);

  // Validation
  const headerCount = rawColumns.length;
  const schemaCount = schema.length;
  const mapCount = mappings.length;

  Logger.info(`Header columns: ${headerCount}`);
  Logger.info(`Schema fields: ${schemaCount}`);
  Logger.info(`Map rows: ${mapCount}`);

  if (schemaCount !== headerCount || mapCount !== headerCount) {
    throw new Error('Count mismatch between header, schema, and mapping');
  }

  // Check for post-sanitization duplicates
  const duplicates = mappings
    .map(m => m.sanitized)
    .filter((name, index, arr) => arr.indexOf(name) !== index);

  if (duplicates.length > 0) {
    Logger.warn('Post-sanitization duplicates were resolved with suffixes:');
    [...new Set(duplicates)].forEach(dup => {
      Logger.warn(`  - ${dup}`);
    });
  }

  // Generate Mixpanel permissions script if mixpanel_project_id is configured
  if (config.mixpanel_project_id) {
    await generateMixpanelPermissionsScript(config);
  }

  Logger.success('Schema preparation complete');
}

async function generateMixpanelPermissionsScript(config) {
  Logger.info('Generating Mixpanel permissions script...');

  const serviceAccount = `project-${config.mixpanel_project_id}@mixpanel-warehouse-1.iam.gserviceaccount.com`;

  const scriptContent = `#!/usr/bin/env bash
set -euo pipefail

# ====== CONFIG ======
PROJECT_ID="${config.project}"
DATASET_ID="${config.dataset}"   # dataset to allow viewing
SERVICE_ACCOUNT="${serviceAccount}" # mixpanel_project_id: ${config.mixpanel_project_id}
LOCATION="${config.location}"                        # set to the dataset's location (e.g., US, EU, us-central1)
# ====================

echo ">>> Ensure project-level Job User (needed to run query jobs; not a data access role)"
gcloud projects add-iam-policy-binding "$PROJECT_ID" \\
  --member="serviceAccount:$SERVICE_ACCOUNT" \\
  --role="roles/bigquery.jobUser" || echo "Job User role may already be assigned"

echo ">>> Grant dataset-scoped Data Viewer via BigQuery DCL (IAM on the dataset only)"
bq --location="$LOCATION" query --nouse_legacy_sql --quiet \\
"GRANT \\\`roles/bigquery.dataViewer\\\`
ON SCHEMA \\\`$PROJECT_ID.$DATASET_ID\\\`
TO \\\"serviceAccount:$SERVICE_ACCOUNT\\\";"

# ---- OPTIONAL: sanity checks ----

echo ">>> Optional: show current dataset-level privileges for this principal"
bq --location="$LOCATION" query --nouse_legacy_sql --quiet \\
"SELECT grantee, role
FROM \\\`region-$LOCATION\\\`.INFORMATION_SCHEMA.SCHEMA_PRIVILEGES
WHERE schema_name = '$DATASET_ID'
  AND table_catalog = '$PROJECT_ID'
  AND grantee = 'serviceAccount:$SERVICE_ACCOUNT';" || true

echo ">>> Optional: try a dry-run query against the dataset to confirm permissions (no cost)"
# Uncomment to test with an actual table:
# bq --location="$LOCATION" query --nouse_legacy_sql --dry_run --quiet "SELECT COUNT(*) FROM \\\`$PROJECT_ID.$DATASET_ID.${config.tables.gold}\\\`"

echo "All done ✅"
`;

  const scriptPath = `${config.paths.tmpDir}/grant-mixpanel-access.sh`;
  await writeFile(scriptPath, scriptContent);

  Logger.success(`Mixpanel permissions script written: ${scriptPath}`);
  Logger.info(`Service Account: ${serviceAccount}`);
  Logger.info(`Grant access with: chmod +x ${scriptPath} && ${scriptPath}`);
}

// Allow running as standalone script
if (import.meta.url === `file://${process.argv[1]}`) {
  try {
    const configData = await readFile('./config.json', 'utf8');
    const config = JSON.parse(configData);

    // Ensure programmatic paths are set for standalone execution
    if (!config.paths.tmpDir) config.paths.tmpDir = './tmp';
    if (!config.paths.lookupsDir) config.paths.lookupsDir = './lookups';

    await prepare(config);
  } catch (error) {
	if (error.code === 'ENOENT') {
	  Logger.error('configuration file config.json not found; please create it based on config.sample.json');
	}
    Logger.error(`Schema preparation failed: ${error.message}`);
    process.exit(1);
  }
}