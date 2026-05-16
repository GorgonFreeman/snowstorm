#!/usr/bin/env node
/**
 * Compare row counts: BigQuery events_intraday_* vs Snowflake GA4_INTRADAY_EVENTS
 * for the same calendar suffixes sync-intraday uses (reporting TZ, last 3 local days).
 *
 * MERGE dedupes on (bq_table_date, user_pseudo_id, event_timestamp, event_name),
 * so Snowflake count should match BQ COUNT(DISTINCT STRUCT(...)), not always BQ COUNT(*).
 */
const { BigQuery } = require('@google-cloud/bigquery');
const snowflake = require('snowflake-sdk');
const {
  loadEnv,
  getBigQueryClientOptions,
  getSnowflakeConnectionOptionsForSync,
} = require('./load-env.js');
const { resolveGa4BigQueryDatasetId } = require('./resolve-ga4-bq-dataset.js');
const { resolveGa4ReportingTimezone } = require('./resolve-ga4-timezone.js');
const { quoteIdent } = require('./snowflake-sql.js');

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

async function bqPartitionStats(bigquery, projectId, datasetId, suffix, location) {
  const tableId = `events_intraday_${ suffix }`;
  const fq = `${ projectId }.${ datasetId }.${ tableId }`;
  const sql = `
    SELECT
      COUNT(*) AS total_rows,
      COUNT(DISTINCT TO_JSON_STRING(STRUCT(
        user_pseudo_id,
        event_timestamp,
        event_name
      ))) AS dedupe_rows
    FROM \`${ fq }\`
  `;
  const jobOpts = { query: sql };
  if (location) {
    jobOpts.location = location;
  }
  const [job] = await bigquery.createQueryJob(jobOpts);
  await job.promise();
  const [rows] = await job.getQueryResults({ maxResults: 1 });
  const r = rows[0] || {};
  const total = Number(r.total_rows ?? r.TOTAL_ROWS);
  const dedupe = Number(r.dedupe_rows ?? r.DEDUPE_ROWS);
  let numBytes;
  try {
    const [meta] = await bigquery.dataset(datasetId).table(tableId).getMetadata();
    numBytes = meta.numBytes != null ? Number(meta.numBytes) : undefined;
  } catch (_) {
    numBytes = undefined;
  }
  return { tableId, total_rows: total, dedupe_rows: dedupe, numBytes };
}

async function sfPartitionCount(conn, suffix) {
  const rows = await runQuery(
    conn,
    'SELECT COUNT(*) AS c FROM GA4_INTRADAY_EVENTS WHERE bq_table_date = ?',
    [suffix],
  );
  const r = rows[0] || {};
  return Number(r.c ?? r.C);
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
  const timeZone = await resolveGa4ReportingTimezone();
  const suffixes = [...intradaySuffixesToSync(timeZone)].sort((a, b) =>
    b.localeCompare(a),
  );

  const bigquery = new BigQuery(bqOpts);
  const datasetId = await resolveGa4BigQueryDatasetId(bigquery);
  let bqLocation;
  try {
    const [meta] = await bigquery.dataset(datasetId).getMetadata();
    bqLocation = meta.location || undefined;
  } catch (_) {
    bqLocation = undefined;
  }

  const conn = await connect(sfOpts);
  try {
    await runQuery(conn, `USE WAREHOUSE ${ quoteIdent(sfOpts.warehouse) }`);
    await runQuery(conn, `USE DATABASE ${ quoteIdent(sfOpts.database) }`);
    await runQuery(conn, `USE SCHEMA ${ quoteIdent(sfOpts.schema) }`);

    console.log('compare-intraday-counts', {
      reportingTimezone: timeZone,
      projectId: bqOpts.projectId,
      datasetId,
      bqLocation: bqLocation || null,
      suffixes,
    });

    let bqTotal = 0;
    let bqDedupe = 0;
    let sfTotal = 0;

    for (const sfx of suffixes) {
      let bq;
      try {
        bq = await bqPartitionStats(
          bigquery,
          bqOpts.projectId,
          datasetId,
          sfx,
          bqLocation,
        );
      } catch (err) {
        const msg = err.message || String(err);
        if (/Not found|not found|404/.test(msg)) {
          console.log('partition', sfx, 'BQ skip (table missing?)', msg.slice(0, 120));
          continue;
        }
        throw err;
      }
      const sf = await sfPartitionCount(conn, sfx);
      bqTotal += bq.total_rows;
      bqDedupe += bq.dedupe_rows;
      sfTotal += sf;

      const matchDedupe = sf === bq.dedupe_rows;
      const matchRaw = sf === bq.total_rows;
      console.log('partition', sfx, {
        bq_total_rows: bq.total_rows,
        bq_dedupe_rows: bq.dedupe_rows,
        bq_table_num_bytes: bq.numBytes ?? null,
        snowflake_rows: sf,
        sf_vs_bq_dedupe: matchDedupe ? 'match' : `diff ${ sf - bq.dedupe_rows }`,
        sf_vs_bq_total: matchRaw ? 'match' : `diff ${ sf - bq.total_rows }`,
      });
    }

    console.log('compare-intraday-counts totals', {
      bq_total_rows_sum: bqTotal,
      bq_dedupe_rows_sum: bqDedupe,
      snowflake_rows_sum: sfTotal,
      sf_vs_bq_dedupe_sum: sfTotal - bqDedupe,
      sf_vs_bq_total_sum: sfTotal - bqTotal,
    });
  } finally {
    await new Promise((resolve) => {
      conn.destroy(() => resolve());
    });
  }
}

main().catch((err) => {
  console.error(err.message || err);
  process.exit(1);
});
