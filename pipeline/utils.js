import { BigQuery } from '@google-cloud/bigquery';
import { Storage } from '@google-cloud/storage';
import { readFile } from 'fs/promises';
import { existsSync } from 'fs';

export class Logger {
  static info(message) {
    console.log(`  ${message}`);
  }

  static success(message) {
    console.log(`\n✅ ${message}\n`);
  }

  static warn(message) {
    console.log(`\n⚠️  ${message}\n`);
  }

  static error(message) {
    console.log(`\n❌ ${message}\n`);
  }
}

export class BigQueryHelper {
  constructor(config) {
    this.config = config;
    this.bq = new BigQuery({
      projectId: config.project,
      location: config.location
    });
    this.dataset = this.bq.dataset(config.dataset);
  }

  async ensureDataset() {
    try {
      await this.dataset.get();
      Logger.success(`Dataset exists: ${this.config.project}:${this.config.dataset}`);
    } catch (error) {
      if (error.code === 404) {
        Logger.info(`Creating dataset ${this.config.project}:${this.config.dataset}...`);
        await this.dataset.create();
        Logger.success(`Dataset created: ${this.config.project}:${this.config.dataset}`);
      } else {
        throw error;
      }
    }
  }

  async tableExists(tableName) {
    try {
      await this.dataset.table(tableName).get();
      return true;
    } catch (error) {
      if (error.code === 404) {
        return false;
      }
      throw error;
    }
  }

  async executeQuery(sql) {
    const [job] = await this.bq.createQueryJob({
      query: sql,
      useLegacySql: false
    });

    Logger.info(`Running BigQuery job: ${job.id}`);
    await job.getQueryResults();
    Logger.success(`Query completed: ${job.id}`);
  }

  async loadTable(tableName, sourceFiles, schema, options = {}) {
    // For GCS URIs, we need to use createLoadJob instead of table.load()
    const sources = Array.isArray(sourceFiles) ? sourceFiles : [sourceFiles];

    const loadOptions = {
      sourceFormat: 'CSV',
      writeDisposition: 'WRITE_TRUNCATE',
      schema: { fields: schema },
      location: this.config.location,
      sourceUris: sources, // Use sourceUris for GCS paths
      destinationTable: {
        projectId: this.config.project,
        datasetId: this.config.dataset,
        tableId: tableName
      },
      ...options
    };

    Logger.info(`Loading table ${tableName} from GCS: ${sources.join(', ')}`);
    Logger.info(`Load format: ${loadOptions.sourceFormat}, delimiter: '${loadOptions.fieldDelimiter}', quote: '${loadOptions.quote}'`);

    try {
      const [job] = await this.bq.createJob({
        configuration: {
          load: loadOptions
        }
      });
      Logger.info(`Loading table ${tableName} with job: ${job.id}`);

      await job.promise();

      // Get job statistics after completion
      const [metadata] = await job.getMetadata();
      const stats = metadata.statistics;

      if (stats && stats.load) {
        Logger.info(`Load statistics for ${tableName}:`);
        Logger.info(`  • Input files: ${stats.load.inputFiles || 'unknown'}`);
        Logger.info(`  • Input bytes: ${stats.load.inputFileBytes || 'unknown'}`);
        Logger.info(`  • Output rows: ${stats.load.outputRows || 'unknown'}`);

        if (stats.load.badRecords && parseInt(stats.load.badRecords) > 0) {
          Logger.warn(`    Bad records encountered: ${stats.load.badRecords} (within tolerance of ${loadOptions.maxBadRecords})`);
        } else {
          Logger.success(`   No bad records encountered`);
        }
      }

      Logger.success(`Table loaded: ${tableName}`);
    } catch (error) {
      Logger.error(`Failed to load table ${tableName}: ${error.message}`);

      // Try to get error details from the job if available
      try {
        const [job] = await this.bq.job(error.jobId || 'unknown');
        const [metadata] = await job.getMetadata();

        if (metadata.status && metadata.status.errors) {
          Logger.error(`Job errors (first 5):`);
          metadata.status.errors.slice(0, 5).forEach((err, i) => {
            Logger.error(`  ${i + 1}. ${err.message}`);
          });

          if (metadata.status.errors.length > 5) {
            Logger.error(`  ... and ${metadata.status.errors.length - 5} more errors`);
          }
        }
      } catch (metaError) {
        // Ignore metadata fetch errors
      }

      throw error;
    }
  }

  async loadLocalTable(tableName, localFilePath, schema, options = {}) {
    const table = this.dataset.table(tableName);

    const loadOptions = {
      sourceFormat: 'CSV',
      writeDisposition: 'WRITE_TRUNCATE',
      schema: { fields: schema },  // Need to wrap in { fields: ... } for table.load()
      location: this.config.location,
      ...options
    };

    Logger.info(`Loading table ${tableName} from local file: ${localFilePath}`);
    Logger.info(`Load format: ${loadOptions.sourceFormat}, delimiter: '${loadOptions.fieldDelimiter}', quote: '${loadOptions.quote}'`);

    try {
      // Use the documented table.load() approach for local files
      const [job] = await table.load(localFilePath, loadOptions);
      Logger.info(`Loading table ${tableName} with job: ${job.id}`);

      // For local file loads, we just wait for the promise to resolve
      // The job object from table.load() resolves when complete
      Logger.info(`Waiting for job completion...`);

      // Simple completion check - just verify the table exists and has data
      let attempts = 0;
      const maxAttempts = 60; // Wait up to 60 seconds
      while (attempts < maxAttempts) {
        try {
          if (await this.tableExists(tableName)) {
            const countSql = `SELECT COUNT(*) as count FROM \`${this.config.project}.${this.config.dataset}.${tableName}\``;
            const [rows] = await this.bq.query(countSql);
            if (rows[0].count > 0) {
              Logger.info(`  • Output rows: ${rows[0].count}`);
              break;
            }
          }
        } catch (error) {
          // Table might not be ready yet, continue waiting
        }
        await new Promise(resolve => setTimeout(resolve, 1000));
        attempts++;
      }

      if (attempts >= maxAttempts) {
        throw new Error(`Job timeout waiting for table ${tableName} to be populated`);
      }

      Logger.success(`Table loaded: ${tableName}`);
    } catch (error) {
      Logger.error(`Failed to load table ${tableName}: ${error.message}`);
      throw error;
    }
  }
}

export class StorageHelper {
  constructor() {
    this.storage = new Storage();
  }

  async checkUriExists(uri) {
    try {
      // Parse gs://bucket/path format
      const match = uri.match(/^gs:\/\/([^\/]+)\/(.+)$/);
      if (!match) {
        throw new Error(`Invalid GCS URI format: ${uri}`);
      }

      let [, bucketName, path] = match;

      // Handle wildcards - strip the wildcard and use the directory as prefix
      if (path.includes('*')) {
        // For gs://bucket/path/to/*.tsv.gz, check gs://bucket/path/to/
        path = path.substring(0, path.lastIndexOf('/') + 1);
        Logger.info(`Checking for files in: gs://${bucketName}/${path} (wildcard pattern)`);
      }

      const bucket = this.storage.bucket(bucketName);
      const [files] = await bucket.getFiles({ prefix: path, maxResults: 5 });

      if (files.length > 0) {
        Logger.info(`Found ${files.length} file(s) in bucket with prefix: ${path}`);
        // Show first few filenames for confirmation
        files.slice(0, 3).forEach(file => {
          Logger.info(`  • ${file.name}`);
        });
        if (files.length > 3) {
          Logger.info(`  ... and ${files.length - 3} more files`);
        }
      }

      return files.length > 0;
    } catch (error) {
      Logger.error(`GCS check failed for ${uri}: ${error.message}`);
      return false;
    }
  }
}

export async function loadSqlTemplate(templatePath, variables = {}) {
  let sql = await readFile(templatePath, 'utf8');

  // Replace ${variable} placeholders
  for (const [key, value] of Object.entries(variables)) {
    sql = sql.replaceAll(`\${${key}}`, value);
  }

  return sql;
}

export function checkFileExists(filePath, description) {
  if (!existsSync(filePath)) {
    throw new Error(`Missing file: ${filePath} (${description})`);
  }
  Logger.success(`Found file: ${filePath}`);
}