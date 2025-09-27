// preprocess.js
import { promises as fs, createReadStream, createWriteStream } from 'fs';
import { dirname, basename, join } from 'path';
import readline from 'readline';
import pLimit from 'p-limit';
import { Storage } from '@google-cloud/storage';
import { Logger } from './utils.js';
import { readFile } from 'fs/promises';
import { createGunzip, createGzip } from 'zlib';

// ------------------------------
// Config
// ------------------------------
const DEFAULT_CONFIG = {
  newlineReplacement: '\\n',
  paddingTolerance: 3,
  parallelism: 1,
  printStats: false, // optional: canonicalTabs, rejectsPath, gcs: { ... }
};

// Adobe Analytics triple-anchor pattern (columns 318-320)
// Allow start-of-line OR tab before hit_time_gmt, to survive physical line splits.
// hit_time_gmt: 10 digits, hitid_high: 10-20 digits, hitid_low: 10-20 digits (flexible for any Adobe data)
const TRIPLE_ANCHOR_RE = /(?:^|\t)([0-9]{10})\t([0-9]{10,20})\t([0-9]{10,20})(?=\t|$)/;
const TRIPLE_ANCHOR_RE_G = new RegExp(TRIPLE_ANCHOR_RE, 'g');

// Expected anchor offset from start-of-record in tabs:
const HIT_TIME_GMT_COLUMN = 318; // 1-based
const ANCHOR_TABS_BEFORE = HIT_TIME_GMT_COLUMN - 1; // 317

// ------------------------------
// IO helpers
// ------------------------------
function isGs(uri) {
  return uri.startsWith('gs://');
}

function looksGzipByName(name) {
  return /\.gz$/i.test(name) || /\.gzip$/i.test(name);
}

function parseGsUri(uri) {
  const [, , bucket, ...rest] = uri.split('/');
  return { bucket, file: rest.join('/') };
}

async function openRead(uri, { autoGunzip = true } = {}) {
  if (isGs(uri)) {
    const storage = new Storage();
    const { bucket, file } = parseGsUri(uri);
    const f = storage.bucket(bucket).file(file);

    let shouldGunzip = autoGunzip && looksGzipByName(file);
    if (autoGunzip && !shouldGunzip) {
      try {
        const [meta] = await f.getMetadata();
        if ((meta.contentEncoding || '').toLowerCase().includes('gzip')) {
          shouldGunzip = true;
        }
      } catch {
        // ignore meta failure; fall back to extension heuristic
      }
    }

    const rs = f.createReadStream();
    return shouldGunzip ? rs.pipe(createGunzip()) : rs;
  }

  const rs = createReadStream(uri);
  return autoGunzip && looksGzipByName(uri) ? rs.pipe(createGunzip()) : rs;
}

async function openWrite(uri, { autoGzip = true } = {}) {
  const shouldGzip = autoGzip && looksGzipByName(uri);

  if (isGs(uri)) {
    const storage = new Storage();
    const { bucket, file } = parseGsUri(uri);
    const ws = storage.bucket(bucket).file(file).createWriteStream({
      resumable: false,
      metadata: shouldGzip ? { contentEncoding: 'gzip' } : undefined,
    });

    if (shouldGzip) {
      const gzipStream = createGzip();
      gzipStream.pipe(ws);
      return gzipStream;
    }
    return ws;
  }

  const dir = dirname(uri);
  await fs.mkdir(dir, { recursive: true });
  const ws = createWriteStream(uri);

  if (shouldGzip) {
    const gzipStream = createGzip();
    gzipStream.pipe(ws);
    return gzipStream;
  }
  return ws;
}

// ------------------------------
// Low-level helpers
// ------------------------------
function countTabs(s) {
  let c = 0;
  for (let i = 0; i < s.length; i++) if (s.charCodeAt(i) === 9) c++;
  return c;
}

// Split a string into [left, right] so that left contains exactly k tabs.
function splitAtTabs(str, k) {
  if (k <= 0) return ['', str];
  let tabs = 0;
  let i = 0;
  for (; i < str.length; i++) {
    if (str.charCodeAt(i) === 9) {
      tabs++;
      if (tabs === k) { i++; break; } // include the k-th tab in left
    }
  }
  // If string has fewer than k tabs, left=whole, right=''
  if (tabs < k) return [str, ''];
  return [str.slice(0, i), str.slice(i)];
}

// ------------------------------
// Discovery: canonical tab count (T)
// ------------------------------
//
// Stream once; gather absolute tab index for each triple-anchor.
// The delta in absolute tabs between consecutive anchors is T.
// We pick the mode delta (break ties by larger delta).
//
export async function discoverCanonicalTabs(uri) {
  const inStream = await openRead(uri);
  const rl = readline.createInterface({ input: inStream, crlfDelay: Infinity });

  let absTabs = 0;         // total tabs seen so far in the file
  const anchorTabIdx = []; // absolute tab index BEFORE the anchor's leading \t (or char 0 if ^)

  for await (const raw of rl) {
    const line = raw.replace(/\r/g, '');
    TRIPLE_ANCHOR_RE_G.lastIndex = 0;

    // Scan all anchors in this physical line
    let m;
    let pos = 0; // char index we've processed in this line
    while ((m = TRIPLE_ANCHOR_RE_G.exec(line)) !== null) {
      const before = line.slice(pos, m.index);
      absTabs += countTabs(before); // ingest tabs up to match
      anchorTabIdx.push(absTabs);   // absolute tabs BEFORE the (optional) leading \t
      pos = m.index;                // don't consume the anchor itself here
      // Advance past the whole match for next search window
      pos = TRIPLE_ANCHOR_RE_G.lastIndex;
    }

    // Ingest remainder of line
    const tail = line.slice(pos);
    absTabs += countTabs(tail);
  }

  if (anchorTabIdx.length < 2) {
    throw new Error('No valid triple anchors found - this may not be an Adobe Analytics file');
  }

  // Build histogram of deltas
  const hist = new Map();
  for (let i = 1; i < anchorTabIdx.length; i++) {
    const delta = anchorTabIdx[i] - anchorTabIdx[i - 1];
    hist.set(delta, (hist.get(delta) || 0) + 1);
  }

  let canonicalTabs = 0;
  let modeFreq = -1;
  for (const [delta, freq] of hist.entries()) {
    if (freq > modeFreq || (freq === modeFreq && delta > canonicalTabs)) {
      canonicalTabs = delta;
      modeFreq = freq;
    }
  }

  if (canonicalTabs <= 0) throw new Error('Failed to determine canonical tab count');

  return {
    canonicalTabs,
    histogram: Object.fromEntries(hist),
    records: anchorTabIdx.length,      // #anchors approximates #records
    validAnchors: anchorTabIdx.length, // for logging
  };
}

// ------------------------------
// Reconstruction (streaming)
// ------------------------------
//
// Maintain a buffer string starting at absolute tab index bufferStartAbsTabs,
// and a running absolute tab count absTabs for the entire file.
// When we hit an anchor at abs index A, the *start* of that record is
// S = A - 317. The boundary (end of previous record) is at S.
// We flush everything up to S (exactly K=S-bufferStartAbsTabs tabs) and
// KEEP the remainder (columns 1..317 of the next record) in the buffer.
// At EOF, flush the final record.
//
// Key changes:
//  - Delay newlineReplacement injection via pendingNL.
//  - When an aligned anchor is seen, split the *current line's* "before" segment
//    at the exact tab count to close the previous record, finalize it, and
//    start the new record with the remainder + anchor. Never slice the global buffer
//    by K tabs.
//
// Assumptions retained:
//  - TRIPLE_ANCHOR_RE_G matches hit_time_gmt (10 digits), hitid_high (10-20 digits), hitid_low (10-20 digits) (cols 318–320).
//  - canonicalTabs = tabs-per-record (T) from discovery (mode of Δ between anchors).
//
// ------------------------------
// Reconstruction (streaming) - ANCHOR-SAFE & MEMORY-BOUNDED
// ------------------------------
async function reconstruct(
  uriIn,
  uriOut,
  rejectsUri,
  canonicalTabs,
  newlineReplacement,
  printStats,
  paddingTolerance = 3
) {
  const inStream  = await openRead(uriIn);
  const outStream = await openWrite(uriOut);
  const rejStream = rejectsUri ? await openWrite(rejectsUri, { autoGzip: false }) : null;

  const rl = readline.createInterface({ input: inStream, crlfDelay: Infinity });

  // Current logical record under construction
  let cur = "";
  let curTabs = 0;

  // Track whether we just crossed a physical newline while continuing the SAME record
  let pendingNL = false;

  // Basic metrics
  let records = 0, ok = 0, bad = 0;

  const writeOut = (s) => outStream.write(s);
  const writeRej = (s) => rejStream && rejStream.write(s);

  // Hard guardrails against pathological growth (and accidental infinite loops)
  const MAX_CUR_LEN = 64 * 1024 * 1024;       // 64 MB of text in one logical row is already extreme
  const MAX_TAB_MULTIPLIER = 8;               // if we ever exceed 8x the expected tabs, bail and reject

  function countTabsLocal(s) {
    let c = 0;
    for (let i = 0; i < s.length; i++) if (s.charCodeAt(i) === 9) c++;
    return c;
  }

  function appendToCur(str, consumePending) {
    if (!str) return;
    if (consumePending && pendingNL) {
      cur += newlineReplacement; // e.g. "\\n"
      pendingNL = false;
    }
    cur += str;
    curTabs += countTabsLocal(str);

    // Safety: if this record is getting absurdly large, reject it to avoid OOM
    if (cur.length > MAX_CUR_LEN || curTabs > canonicalTabs * MAX_TAB_MULTIPLIER) {
      writeRej(cur + "\n"); bad++;
      cur = ""; curTabs = 0; pendingNL = false;
    }
  }

  function looksLikeFragment(tabsInRow) {
    if (tabsInRow < 10) return true;
    if (tabsInRow < Math.floor(canonicalTabs * 0.6)) return true;
    return false;
  }

  function finalizeCur() {
    if (!cur) return;
    records++;

    const cols = curTabs + 1;               // tabs + 1 = columns
    const targetCols = canonicalTabs + 1;

    if (curTabs === canonicalTabs) {
      writeOut(cur + "\n"); ok++;
    } else if (cols < targetCols && (targetCols - cols) <= paddingTolerance) {
      // Slightly short: pad with tabs
      writeOut(cur + "\t".repeat(targetCols - cols) + "\n"); ok++;
    } else if (cols > targetCols && (cols - targetCols) <= paddingTolerance) {
      // Slightly long: trim extra columns
      const parts = cur.split("\t");
      writeOut(parts.slice(0, targetCols).join("\t") + "\n"); ok++;
    } else if (looksLikeFragment(curTabs)) {
      writeRej(cur + "\n"); bad++;
    } else {
      writeRej(cur + "\n"); bad++;
    }

    cur = "";
    curTabs = 0;
    // caller owns pendingNL; do not touch it here
  }

  for await (const rawLine of rl) {
    // Normalize CRLF → LF, keep everything else verbatim
    const line = rawLine.replace(/\r/g, "");

    // We're starting a new physical line. If we're *already* building a record,
    // remember to inject newlineReplacement *once* before we append this line's
    // first byte of content that belongs to the same record.
    pendingNL = (cur.length > 0);

    // Append the entire physical line to the current logical record
    appendToCur(line, true);

    // Heuristic for when to emit a record:
    // - If we have accumulated at least the canonical number of tabs,
    //   it's time to close. (Most rows will hit ==; > means malformed,
    //   but we still finalize and let finalizeCur decide keep/reject.)
    if (curTabs >= canonicalTabs) {
      finalizeCur();
      // After finalizing, we are between records at a physical newline, so clear pendingNL.
      pendingNL = false;
    } else {
      // Not enough columns yet: keep accumulating; on the next line,
      // we'll insert newlineReplacement before appending any content.
      // pendingNL remains true until we actually append more content.
    }
  }

  // EOF: emit any remaining partial record
  if (cur) finalizeCur();

  if (printStats) {
    Logger.info(`Processed ${uriIn}: ${records} records, ${ok} ok, ${bad} rejected, canonical tabs: ${canonicalTabs}`);
  }

  await new Promise((r) => outStream.end(r));
  if (rejStream) await new Promise((r) => rejStream.end(r));

  return { records, ok, bad, canonicalTabs };
}


// ------------------------------
// Public API
// ------------------------------
export async function preprocessFile(inputPath, outputPath, options = {}) {
  const config = { ...DEFAULT_CONFIG, ...options };

  Logger.info(`Preprocessing ${inputPath} -> ${outputPath}`);

  let canonicalTabs;
  if (config.canonicalTabs !== undefined) {
    canonicalTabs = config.canonicalTabs;
  } else {
    const { canonicalTabs: T, records, validAnchors, histogram } = await discoverCanonicalTabs(inputPath);
    canonicalTabs = T;
    if (config.printStats) {
      Logger.info(`Discovered canonical tabs (mode Δ between anchors): ${canonicalTabs} (anchors=${validAnchors})`);
      const top = Object.entries(histogram).sort((a, b) => b[1] - a[1]).slice(0, 5).map(([d, f]) => `${d}:${f}`).join(', ');
      Logger.info(`Top deltas: ${top}`);
    }
  }

  // Rejects path: {filename}-rejected.tsv (ungzipped for easy reading)
  const rejectsPath = config.rejectsPath || (() => {
    const base = basename(outputPath);
    const dir = dirname(outputPath);
    const name = base.replace(/(\.[^.]+)?(\.[^.]+)?$/, '-rejected$1').replace(/\.gz$/, '');
    return join(dir, name);
  })();

  const result = await reconstruct(
    inputPath,
    outputPath,
    rejectsPath,
    canonicalTabs,
    config.newlineReplacement,
    config.printStats,
    config.paddingTolerance
  );

  Logger.success(
    `Preprocessed ${inputPath}: ${result.ok}/${result.records} records processed${result.bad ? `, ${result.bad} rejected` : ''}`
  );
  return result;
}

// ------------------------------
// Local folder helpers
// ------------------------------
async function findFiles(pattern) {
  if (!pattern.includes('*') && !pattern.includes('?')) return [pattern];

  if (pattern.endsWith('/*.tsv') || pattern.endsWith('/*.tsv.gz')) {
    const dir = pattern.replace(/\/\*\.tsv(\.gz)?$/, '');
    const wantGz = pattern.endsWith('.gz');
    const files = await fs.readdir(dir).catch(err => {
      throw new Error(`Cannot read directory ${dir}: ${err.message}`);
    });
    return files
      .filter(f => wantGz ? f.endsWith('.tsv.gz') : f.endsWith('.tsv'))
      .map(f => join(dir, f));
  }

  throw new Error(`Complex glob patterns not supported: ${pattern}. Use "some/dir/*.tsv" or "some/dir/*.tsv.gz".`);
}

export async function preprocessFolder(inputPattern, outputDir, options = {}) {
  const config = { ...DEFAULT_CONFIG, ...options };
  const files = await findFiles(inputPattern);
  if (!files.length) throw new Error(`No files found for pattern: ${inputPattern}`);

  Logger.info(`Found ${files.length} files to preprocess`);
  await fs.mkdir(outputDir, { recursive: true });

  const limit = pLimit(config.pipeline_config?.parallelism ?? config.parallelism ?? DEFAULT_CONFIG.parallelism);
  const tasks = files.map(file => {
    const out = join(outputDir, basename(file));
    return limit(() => preprocessFile(file, out, config).then(result => ({ file, result })));
  });

  const results = await Promise.all(tasks);

  const totals = results.reduce((acc, { result }) => {
    acc.records += result.records; acc.ok += result.ok; acc.bad += result.bad;
    return acc;
  }, { records: 0, ok: 0, bad: 0 });

  Logger.success(`Preprocessed ${files.length} files: ${totals.ok}/${totals.records} records processed, ${totals.bad} rejected`);
  return results;
}

// ------------------------------
// GCS wildcard expansion + pipeline
// ------------------------------
async function expandGcsWildcard(pattern) {
  const m = pattern.match(/^gs:\/\/([^\/]+)\/(.+)$/);
  if (!m) throw new Error(`Invalid GCS URI: ${pattern}`);
  const [, bucketName, path] = m;

  if (!path.includes('*') && !path.includes('?')) return [pattern];

  const lastSlash = path.lastIndexOf('/');
  const directory = lastSlash >= 0 ? path.substring(0, lastSlash + 1) : '';
  const filePattern = lastSlash >= 0 ? path.substring(lastSlash + 1) : path;

  const storage = new Storage();
  const bucket = storage.bucket(bucketName);
  const [files] = await bucket.getFiles({ prefix: directory });

  const regexPattern = filePattern
    .replace(/\./g, '\\.')
    .replace(/\*/g, '.*')
    .replace(/\?/g, '.');
  const regex = new RegExp(`^${directory}${regexPattern}$`);

  const uris = files
    .filter(f => regex.test(f.name))
    .map(f => `gs://${bucketName}/${f.name}`);

  Logger.info(`Found ${uris.length} files for pattern: ${pattern}`);
  return uris;
}

export async function preprocess(config) {
  Logger.info('=== Preprocessing Phase ===');

  if (!config?.gcs?.transformDest) {
    throw new Error('transformDest must be configured in config.gcs.transformDest');
  }

  const inputPattern = config.gcs.sourceUri;   // e.g., gs://bucket/path/*.tsv.gz
  const outputDir   = config.gcs.transformDest;  // e.g., gs://bucket/cleaned/
  const rejectsDir  = config.gcs.rejectsDest || outputDir;

  // Safety: prevent overwrite loops
  if (inputPattern === outputDir || outputDir.startsWith(inputPattern.replace('*', ''))) {
    throw new Error('transformDest cannot be the same as or overlap with sourceUri');
  }

  const inputFiles = await expandGcsWildcard(inputPattern);
  if (!inputFiles.length) {
    Logger.warn('No files found matching input pattern');
    return;
  }

  Logger.info(`Preprocessing from: ${inputPattern}`);
  Logger.info(`Preprocessing to:   ${outputDir}`);

  const parallelism = config.pipeline_config?.parallelism ?? config.parallelism ?? DEFAULT_CONFIG.parallelism;
  const limit = pLimit(parallelism);

  const tasks = inputFiles.map(inputFile => {
    const fileName = basename(inputFile);
    const outputFile = outputDir.endsWith('/')
      ? `${outputDir}${fileName}`
      : `${outputDir}/${fileName}`;

    // Rejects: file.tsv.gz -> file-rejected.tsv (plain .tsv)
    const rejectFileName = fileName.replace(/(\.[^.]+)?(\.[^.]+)?$/, '-rejected$1').replace(/\.gz$/, '');
    const rejectsPath = rejectsDir.endsWith('/')
      ? `${rejectsDir}${rejectFileName}`
      : `${rejectsDir}/${rejectFileName}`;

    return limit(() =>
      preprocessFile(inputFile, outputFile, {
        ...config,
        rejectsPath,
        printStats: false
      }).then(result => ({ inputFile, outputFile, result }))
    );
  });

  const results = await Promise.all(tasks);

  const totals = results.reduce((acc, { result }) => {
    acc.records += result.records; acc.ok += result.ok; acc.bad += result.bad;
    return acc;
  }, { records: 0, ok: 0, bad: 0 });

  Logger.success(`Preprocessed ${results.length} files: ${totals.ok}/${totals.records} records processed${totals.bad ? `, ${totals.bad} rejected` : ''}`);
}

// ------------------------------
// CLI
// ------------------------------
if (import.meta.url === `file://${process.argv[1]}`) {
  try {
    const configData = await readFile('./config.json', 'utf8');
    const config = JSON.parse(configData);
    await preprocess(config);
  } catch (err) {
    Logger.error(`Preprocessing failed: ${err.message}`);
    process.exit(1);
  }
}
