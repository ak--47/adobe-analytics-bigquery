# Adobe Analytics Data Transformation Pipeline

A Node.js-based ETL pipeline for transforming raw Adobe Analytics exports into clean, denormalized datasets ready for analysis in BigQuery.

## Overview

This pipeline processes compressed TSV files from Adobe Analytics data feeds, applying comprehensive data cleaning, event parsing, and lookup table joins to produce a "golden" dataset with human-readable labels and properly structured event data. Built with Node.js and ES modules for maintainability and reliability.

## Quick Start

1. **Install dependencies**:
   ```bash
   npm install
   ```

2. **Configure your pipeline** by editing `config.json`:
   ```json
   {
     "project": "your-gcp-project",
     "dataset": "your_dataset",
     "gcs": {
       "sourceUri": "gs://your-bucket/path/*.tsv.gz",
       "exportPrefix": "gs://your-bucket/exports/golden/part-*"
     }
   }
   ```

3. **Run the full pipeline**:
   ```bash
   npm run pipeline
   ```

   Or run individual steps:
   ```bash
   npm run validate
   npm run prepare
   npm run load
   npm run transform
   npm run unload
   ```

## Pipeline Steps

| Module | Purpose | Input | Output |
|--------|---------|-------|--------|
| `src/validate.js` | Environment validation, dependency checks | Configuration | Validation report |
| `src/prepare.js` | Generate BigQuery schema from CSV header | `lookups/columns.csv` | Schema JSON + mapping |
| `src/load.js` | Load data and lookup tables with column drift validation | Raw data + lookup files | BigQuery tables |
| `src/transform.js` | Main data transformation (Bronze → Gold) | Raw data + lookups | Golden dataset |
| `src/unload.js` | Export final dataset | Golden dataset | GCS JSON files |

## Data Architecture

### Input Data
- **Raw Adobe Data**: Compressed TSV files in GCS
- **Column Schema**: `lookups/columns.csv` defines the expected column structure
- **Lookup Tables**: Various TSV files for mapping numeric codes to readable labels

### Transformation Layers
1. **Native**: Raw Adobe data loaded as-is
2. **Bronze**: String normalization (NULLs empty values, standardized missing data markers)
3. **Gold**: Full transformation with event parsing, visitor ID consolidation, and denormalized lookups

### Key Features
- **Event Processing**: Parses comma-separated event lists with support for custom events and eVars
- **SDR Integration**: Solution Design Reference overrides for custom event and eVar names
- **Visitor ID Logic**: Adobe-style visitor precedence (Analytics ID → Marketing Cloud → Custom → Fingerprint)
- **Timestamp Handling**: Supports both 10-digit (seconds) and 13-digit (milliseconds) formats
- **Page View Detection**: Multiple methods for identifying page view hits
- **Data Denormalization**: Replaces numeric codes with human-readable labels

## Directory Structure

```
├── package.json           # Node.js project configuration
├── config.json           # Pipeline configuration
├── src/                  # Node.js modules
│   ├── validate.js       # Environment validation
│   ├── prepare.js        # Schema generation
│   ├── load.js           # Data loading with column drift validation
│   ├── transform.js      # Main transformation
│   ├── unload.js         # Data export
│   └── utils.js          # Shared utilities
├── models/               # SQL templates
│   ├── create-bronze.sql
│   ├── create-gold.sql
│   └── create-sdr-maps.sql
├── archived/             # Original bash scripts (for reference)
└── lookups/
    ├── columns.csv       # Column definitions
    ├── events.tsv        # Event code mappings
    ├── sdr/             # Solution Design Reference
    │   ├── custom-events.tsv
    │   ├── custom-props.tsv
    │   └── evars.tsv
    └── values/          # Enumerated lookups
        ├── browser.tsv
        ├── country.tsv
        ├── operating_systems.tsv
        └── ... (12 more lookup files)
```

## Configuration

### config.json Structure
```json
{
  "project": "your-gcp-project",
  "location": "US",
  "dataset": "your_dataset",
  "tables": {
    "raw": "your_raw_table",
    "gold": "your_gold_table"
  },
  "gcs": {
    "sourceUri": "gs://bucket/path/*.tsv.gz",
    "exportPrefix": "gs://bucket/exports/part-*"
  },
  "paths": {
    "tmpDir": "./tmp",
    "columnsFile": "./lookups/columns.csv",
    "schemaJson": "./tmp/schema.json",
    "sanitizeMap": "./tmp/column_sanitization_map.tsv"
  },
  "validation": {
    "expectedColumns": 1189
  },
  "dateFilter": {
    "startDate": "2023-01-01",
    "endDate": "2023-12-31"
  }
}
```

### Dependencies
- Node.js (v18+)
- Google Cloud credentials (Application Default Credentials)
- `@google-cloud/bigquery` and `@google-cloud/storage` (installed via npm)

## Data Quality Features

- **Column Sanitization**: Standardizes column names for BigQuery compatibility
- **Duplicate Handling**: Automatically suffixes duplicate column names
- **Missing Data Normalization**: Converts various empty value representations to NULL
- **Column Drift Protection**: Advanced validation prevents TSV parsing errors that misalign columns
  - Disables quoting (`quote: ''`) to handle stray quotes in Adobe data
  - Validates accept_language field contains actual language codes, not page titles
  - Fails fast (`maxBadRecords: 0`) on any malformed rows
- **Validation Checks**: Comprehensive pre-flight checks for data integrity
- **Type Safety**: Proper casting and error handling for numeric conversions

## Usage Examples

### Run complete pipeline:
```bash
npm run pipeline
```

### Run individual steps:
```bash
# 1. Generate schemas (REQUIRED FIRST)
npm run prep

# 2. Clean TSV data (OPTIONAL - only if you have embedded newlines/tabs)
npm run preprocess

# 3. Validate environment and configuration
npm run validate

# 4. Load raw data and lookup tables
npm run load

# 5. Transform through Bronze → Silver → Gold layers
npm run bronze
npm run silver
npm run gold

# Or run all transformations at once:
npm run transform

# 6. Export final dataset
npm run unload

# Clean up temporary files
npm run prune

# Reset all tables for fresh start
npm run reset
```

### Run individual modules directly (debugging):
```bash
node pipeline/0-prepare.js
node pipeline/0a-preprocess.js
node pipeline/1-validate.js
node pipeline/2-load.js
node pipeline/3-transform-bronze.js
node pipeline/3-transform-silver.js
node pipeline/3-transform-gold.js
node pipeline/4-unload.js
node pipeline/5-reset.js
```

## Pipeline Architecture

This pipeline transforms Adobe Analytics data through 4 distinct layers:

1. **Raw** - Direct TSV load with all original columns (~1189 columns)
2. **Bronze** - Parsed timestamps, visitor IDs, and event arrays (+3 columns)
3. **Silver** - SDR mappings and business-friendly column names (+20 columns)
4. **Gold** - Eventified data with column optimization (~600 essential columns)

**For detailed technical architecture, layer transformations, and validation queries, see [ARCH.md](./ARCH.md).**

## Final Output Schema

The Gold layer produces an eventified dataset where each row represents a single event:
- **Core Fields**: `ts_utc`, `distinct_id`, `event`, `is_page_view`
- **Page Context**: `page_url`, `page_name`, `page_title`
- **Visitor Context**: Denormalized browser, OS, country, visitor type
- **Event Properties**: Measurements (page load time, scroll depth, etc.)
- **Business Properties**: SDR-mapped eVar and prop values with human-readable names

## Troubleshooting

- **Schema mismatches**: Check `validation.expectedColumns` in config.json vs actual column count
- **Missing lookup files**: Verify all files in `lookups/` directory exist
- **GCS access issues**: Ensure proper IAM permissions for source and destination buckets
- **BigQuery errors**: Check dataset exists and has appropriate permissions
- **Column drift errors**: Pipeline will halt if accept_language contains page titles instead of language codes
  - This indicates TSV parsing issues during data load
  - Check for corrupted source files or quote handling problems
- **Authentication**: Ensure Application Default Credentials are configured (`gcloud auth application-default login`)
- **Node.js errors**: Requires Node.js v18+ for top-level await support


### RESEARCH
https://experienceleague.adobe.com/en/docs/analytics/export/analytics-data-feed/data-feed-contents/datafeeds-contents
https://experienceleague.adobe.com/en/docs/experience-platform/sources/connectors/adobe-applications/mapping/analytics