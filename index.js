#!/usr/bin/env node

import { readFile } from 'fs/promises';
import { prepare } from './pipeline/0-prepare.js';
import { preprocess } from './pipeline/0a-preprocess.js';
import { validate } from './pipeline/1-validate.js';
import { load } from './pipeline/2-load.js';
import { transformToBronze } from './pipeline/3-transform-bronze.js';
import { transformToSilver } from './pipeline/3-transform-silver.js';
import { transformToGold } from './pipeline/3-transform-gold.js';
import { unload } from './pipeline/4-unload.js';

async function loadConfig() {
  const configData = await readFile('./config.json', 'utf8');
  return JSON.parse(configData);
}

async function main() {
  try {
    console.log('🚀 Starting Adobe Analytics transformation pipeline...\n');

    const config = await loadConfig();

    console.log('🛠️  Step 1: Prepare schema (REQUIRED FIRST)');
    await prepare(config);
    console.log('✅ Schema preparation complete\n');

    // console.log('🔧 Step 2: Preprocess TSV files (OPTIONAL)');
    // if (config.gcs.transformDest) {
    //   await preprocess(config);
    //   console.log('✅ Preprocessing complete\n');
    // } else {
    //   console.log('⏭️  Skipping preprocessing (transformDest not configured)\n');
    // }

    console.log('📋 Step 3: Validation');
    await validate(config);
    console.log('✅ Validation complete\n');

    console.log('📤 Step 4: Load raw data and lookups');
    await load(config);
    console.log('✅ Loading complete\n');

    console.log('🥉 Step 5: Transform raw to bronze');
    await transformToBronze(config);
    console.log('✅ Bronze transformation complete\n');

    console.log('🥈 Step 6: Transform bronze to silver (SDR mappings)');
    await transformToSilver(config);
    console.log('✅ Silver transformation complete\n');

    console.log('🥇 Step 7: Transform silver to gold (eventification)');
    await transformToGold(config);
    console.log('✅ Gold transformation complete\n');

    // console.log('📦 Step 8: Export gold data');
    // await unload(config);
    // console.log('✅ Export complete\n');

    console.log('🎉 Pipeline completed successfully!');

  } catch (error) {
    console.error('❌ Pipeline failed:', error.message);
    process.exit(1);
  }
}

// Allow running as main script
if (import.meta.url === `file://${process.argv[1]}`) {
  main();
}