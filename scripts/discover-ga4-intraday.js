#!/usr/bin/env node
/**
 * Lists GA4 streaming intraday tables (events_intraday_YYYYMMDD) in BigQuery
 * using the Cloud BigQuery API (@google-cloud/bigquery).
 *
 * Env: GOOGLE_CLOUD_PROJECT (or GCP_PROJECT), GOOGLE_SERVICE_ACCOUNT_JSON,
 *      optional GA4_BIGQUERY_DATASET.
 */
const { BigQuery } = require('@google-cloud/bigquery');
const { loadEnv, getBigQueryClientOptions } = require('./load-env.js');

const INTRADAY_PREFIX = 'events_intraday_';

function parseTableDate(tableId) {
  const suffix = tableId.slice(INTRADAY_PREFIX.length);
  if (!/^\d{8}$/.test(suffix)) {
    return null;
  }
  const y = suffix.slice(0, 4);
  const m = suffix.slice(4, 6);
  const d = suffix.slice(6, 8);
  return `${ y }-${ m }-${ d }`;
}

function sortNewestFirst(rows) {
  return [...rows].sort((a, b) => String(b.tableId).localeCompare(String(a.tableId)));
}

async function listIntradayInDataset(bigquery, datasetId) {
  const dataset = bigquery.dataset(datasetId);
  const [tables] = await dataset.getTables({ autoPaginate: true });
  const intraday = [];
  for (const t of tables) {
    const tableId = t.id;
    if (!tableId || !tableId.startsWith(INTRADAY_PREFIX)) {
      continue;
    }
    const eventDate = parseTableDate(tableId);
    intraday.push({
      datasetId,
      tableId,
      eventDate,
      fullId: `${ datasetId }.${ tableId }`,
    });
  }
  return sortNewestFirst(intraday);
}

async function listIntradayAcrossProject(bigquery) {
  const [datasets] = await bigquery.getDatasets({ autoPaginate: true });
  const all = [];
  for (const ds of datasets) {
    const datasetId = ds.id;
    if (!datasetId) {
      continue;
    }
    const found = await listIntradayInDataset(bigquery, datasetId);
    all.push(...found);
  }
  return sortNewestFirst(all);
}

async function main() {
  loadEnv();

  let clientOpts;
  try {
    clientOpts = getBigQueryClientOptions();
  } catch (e) {
    console.error(e.message || e);
    process.exit(1);
    return;
  }

  const projectId = clientOpts.projectId;
  const focusedDataset = process.env.GA4_BIGQUERY_DATASET?.trim() || null;
  const bigquery = new BigQuery(clientOpts);

  let rows;
  if (focusedDataset) {
    rows = await listIntradayInDataset(bigquery, focusedDataset);
    console.log(
      `Project ${ projectId }, dataset ${ focusedDataset }: ${ rows.length } ${ INTRADAY_PREFIX }* table(s)`,
    );
  } else {
    rows = await listIntradayAcrossProject(bigquery);
    console.log(
      `Project ${ projectId } (all datasets): ${ rows.length } ${ INTRADAY_PREFIX }* table(s)`,
    );
  }

  if (rows.length === 0) {
    console.log(
      'No intraday tables found. Confirm GA4 → BigQuery streaming export is enabled and tables are named events_intraday_YYYYMMDD.',
    );
    process.exit(0);
    return;
  }

  const preview = rows.slice(0, 20);
  for (const r of preview) {
    console.log(`  ${ r.fullId }${ r.eventDate ? `  (event date ${ r.eventDate })` : '' }`);
  }
  if (rows.length > preview.length) {
    console.log(`  … and ${ rows.length - preview.length } more`);
  }

  const latest = rows[0];
  if (latest) {
    const table = bigquery.dataset(latest.datasetId).table(latest.tableId);
    const [meta] = await table.getMetadata();
    const numBytes = meta.numBytes != null ? Number(meta.numBytes) : null;
    const numRows = meta.numRows != null ? Number(meta.numRows) : null;
    console.log('\nLatest table metadata (API):');
    console.log('  fullId', latest.fullId);
    if (numRows != null) {
      console.log('  numRows', numRows);
    }
    if (numBytes != null) {
      console.log('  numBytes', numBytes);
    }
  }
}

main().catch((err) => {
  console.error(err.message || err);
  process.exit(1);
});
