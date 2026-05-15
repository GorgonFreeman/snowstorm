#!/usr/bin/env node
/**
 * Incremental sync: BigQuery `events_intraday_YYYYMMDD` → Snowflake GA4_INTRADAY_EVENTS
 * with MERGE dedupe (user_pseudo_id + event_timestamp + event_name + bq partition).
 *
 * Schedule a few times per day (e.g. Cloud Scheduler → Cloud Run). Requires onboard DDL
 * and .env: Snowflake warehouse/db/schema, GOOGLE_SERVICE_ACCOUNT_JSON.
 * GA4 dataset is resolved via BigQuery API (GA4_BIGQUERY_DATASET optional override).
 */
const fs = require('fs');
const os = require('os');
const path = require('path');
const { pathToFileURL } = require('url');
const { BigQuery } = require('@google-cloud/bigquery');
const snowflake = require('snowflake-sdk');
const {
  loadEnv,
  getBigQueryClientOptions,
  getSnowflakeConnectionOptionsForSync,
  getGa4ReportingTimezone,
} = require('./load-env.js');
const { resolveGa4BigQueryDatasetId } = require('./resolve-ga4-bq-dataset.js');
const { quoteIdent } = require('./snowflake-sql.js');

const BATCH_LIMIT = Number(process.env.GA4_INTRADAY_BATCH_ROWS || 250000);
const MAX_BATCHES_PER_PARTITION = Number(process.env.GA4_INTRADAY_MAX_BATCHES || 80);

function yyyymmddInZone(date, timeZone) {
  const parts = new Intl.DateTimeFormat('en-CA', {
    timeZone,
    year: 'numeric',
    month: '2-digit',
    day: '2-digit',
  }).formatToParts(date);
  const y = parts.find((p) => p.type === 'year').value;
  const m = parts.find((p) => p.type === 'month').value;
  const d = parts.find((p) => p.type === 'day').value;
  return `${ y }${ m }${ d }`;
}

function intradaySuffixesToSync(timeZone) {
  const now = Date.now();
  const days = [0, 1, 2].map((i) => new Date(now - i * 24 * 60 * 60 * 1000));
  const sfx = days.map((d) => yyyymmddInZone(d, timeZone));
  return [...new Set(sfx)];
}

function connect(opts) {
  return new Promise((resolve, reject) => {
    const c = snowflake.createConnection(opts);
    c.connect((err) => {
      if (err) {
        reject(err);
        return;
      }
      resolve(c);
    });
  });
}

function runQuery(conn, sqlText, binds) {
  return new Promise((resolve, reject) => {
    const payload = {
      sqlText,
      complete: (err, _stmt, rows) => {
        if (err) {
          reject(err);
          return;
        }
        resolve(rows || []);
      },
    };
    if (binds && binds.length) {
      payload.binds = binds;
    }
    conn.execute(payload);
  });
}

function pickWatermark(row) {
  if (!row) {
    return -1;
  }
  const v =
    row.WATERMARK_EVENT_TIMESTAMP
    ?? row.watermark_event_timestamp;
  if (v == null) {
    return -1;
  }
  const n = Number(v);
  return Number.isFinite(n) ? n : -1;
}

async function loadWatermark(conn, syncKey) {
  const rows = await runQuery(
    conn,
    'SELECT watermark_event_timestamp FROM GA4_INTRADAY_SYNC_STATE WHERE sync_key = ?',
    [syncKey],
  );
  return pickWatermark(rows[0]);
}

async function saveWatermark(conn, syncKey, watermark) {
  await runQuery(
    conn,
    `MERGE INTO GA4_INTRADAY_SYNC_STATE t
     USING (SELECT ? AS sk, ? AS wm) s
     ON t.sync_key = s.sk
     WHEN MATCHED THEN
       UPDATE SET watermark_event_timestamp = s.wm, updated_at = CURRENT_TIMESTAMP()
     WHEN NOT MATCHED THEN
       INSERT (sync_key, watermark_event_timestamp) VALUES (s.sk, s.wm)`,
    [syncKey, watermark],
  );
}

async function flushBatchToSnowflake(conn, rows, bqTableDate) {
  if (!rows.length) {
    return;
  }
  const lines = rows.map((r) => {
    let raw;
    try {
      raw = JSON.parse(r.row_json);
    } catch {
      raw = {};
    }
    return `${ JSON.stringify({
      raw,
      bq_table_date: bqTableDate,
    }) }\n`;
  });
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'snowstorm-ga4-'));
  const filePath = path.join(dir, `batch-${ process.pid }-${ Date.now() }.json`);
  fs.writeFileSync(filePath, lines.join(''), 'utf8');
  const fileUrl = pathToFileURL(filePath).href;

  await runQuery(
    conn,
    `PUT ${ fileUrl } @%GA4_INTRADAY_EVENTS AUTO_COMPRESS=TRUE OVERWRITE=TRUE`,
  );

  await runQuery(
    conn,
    'CREATE OR REPLACE TEMP TABLE GA4_INTRADAY_STAGING (LIKE GA4_INTRADAY_EVENTS)',
  );

  await runQuery(
    conn,
    `COPY INTO GA4_INTRADAY_STAGING (raw, bq_table_date)
     FROM @%GA4_INTRADAY_EVENTS
     FILE_FORMAT = (FORMAT_NAME = 'SNOWSTORM_NDJSON_FMT')
     MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
     ON_ERROR = 'ABORT_STATEMENT'`,
  );

  await runQuery(
    conn,
    `MERGE INTO GA4_INTRADAY_EVENTS t
     USING GA4_INTRADAY_STAGING s
     ON t.bq_table_date = s.bq_table_date
       AND t.raw:"user_pseudo_id"::STRING = s.raw:"user_pseudo_id"::STRING
       AND t.raw:"event_timestamp"::BIGINT = s.raw:"event_timestamp"::BIGINT
       AND t.raw:"event_name"::STRING = s.raw:"event_name"::STRING
     WHEN NOT MATCHED THEN
       INSERT (raw, bq_table_date) VALUES (s.raw, s.bq_table_date)`,
  );

  await runQuery(conn, 'REMOVE @%GA4_INTRADAY_EVENTS');

  fs.unlinkSync(filePath);
  try {
    fs.rmdirSync(dir);
  } catch (_) {
    /* ignore */
  }
}

async function syncPartition({
  bigquery,
  conn,
  projectId,
  datasetId,
  tableSuffix,
}) {
  const intradayTable = `events_intraday_${ tableSuffix }`;
  const fq = `\`${ projectId }.${ datasetId }.${ intradayTable }\``;
  const syncKey = `intraday|${ datasetId }|${ tableSuffix }`;

  let watermark = await loadWatermark(conn, syncKey);
  let batchCount = 0;
  let total = 0;

  while (batchCount < MAX_BATCHES_PER_PARTITION) {
    const cap = Math.max(1, Math.min(1_000_000, Math.floor(BATCH_LIMIT)));
    const query = `
      SELECT event_timestamp, TO_JSON_STRING(t) AS row_json
      FROM ${ fq } t
      WHERE event_timestamp > @wm
      ORDER BY event_timestamp
      LIMIT ${ cap }
    `;
    let rows;
    try {
      [rows] = await bigquery.query({
        query,
        params: { wm: watermark },
      });
    } catch (err) {
      const msg = err.message || String(err);
      if (/Not found|not found|404/.test(msg)) {
        console.log('syncPartition skip (BQ)', intradayTable, msg);
        return 0;
      }
      throw err;
    }

    if (!rows.length) {
      break;
    }

    await flushBatchToSnowflake(conn, rows, tableSuffix);
    const maxTs = Math.max(...rows.map((r) => Number(r.event_timestamp)));
    watermark = maxTs;
    await saveWatermark(conn, syncKey, watermark);
    total += rows.length;
    batchCount += 1;
    console.log('syncPartition', intradayTable, 'batchRows', rows.length, 'watermark', watermark);

    if (rows.length < cap) {
      break;
    }
  }

  return total;
}

async function main() {
  loadEnv();
  try {
    snowflake.configure({ logLevel: 'ERROR' });
  } catch (_) {
    /* ignore */
  }

  const bqOpts = getBigQueryClientOptions();
  const sfOpts = getSnowflakeConnectionOptionsForSync();
  const timeZone = getGa4ReportingTimezone();
  const tableSuffixes = intradaySuffixesToSync(timeZone);

  const bigquery = new BigQuery(bqOpts);
  const datasetId = await resolveGa4BigQueryDatasetId(bigquery);
  console.log('sync-intraday datasetId', datasetId);
  let conn;
  try {
    conn = await connect(sfOpts);

    await runQuery(conn, `USE WAREHOUSE ${ quoteIdent(sfOpts.warehouse) }`);
    await runQuery(conn, `USE DATABASE ${ quoteIdent(sfOpts.database) }`);
    await runQuery(conn, `USE SCHEMA ${ quoteIdent(sfOpts.schema) }`);

    let grand = 0;
    const ordered = [...tableSuffixes].sort((a, b) => b.localeCompare(a));
    for (const sfx of ordered) {
      const n = await syncPartition({
        bigquery,
        conn,
        projectId: bqOpts.projectId,
        datasetId,
        tableSuffix: sfx,
      });
      grand += n;
    }

    console.log('sync-intraday done, totalRows', grand, 'suffixes', ordered.join(', '));
  } finally {
    if (conn) {
      await new Promise((resolve) => {
        conn.destroy(() => resolve());
      });
    }
  }
}

main().catch((err) => {
  console.error(err.message || err);
  process.exit(1);
});
