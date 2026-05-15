/**
 * Resolve the GA4 BigQuery export dataset id via the BigQuery API (no manual naming).
 *
 * Order:
 * 1) GA4_BIGQUERY_DATASET — must exist in the configured GCP project
 * 2) analytics_${ GA4_PROPERTY_ID } — if that dataset exists
 * 3) Exactly one dataset in the project containing tables named events_intraday_*
 *    (multiple → error; ask user to set GA4_BIGQUERY_DATASET)
 */
const INTRADAY_PREFIX = 'events_intraday_';

async function datasetExists(bigquery, datasetId) {
  const [exists] = await bigquery.dataset(datasetId).exists();
  return exists;
}

async function datasetHasIntradayTable(bigquery, datasetId) {
  const [tables] = await bigquery.dataset(datasetId).getTables({ autoPaginate: true });
  return tables.some((t) => t.id && t.id.startsWith(INTRADAY_PREFIX));
}

async function listDatasetsWithIntraday(bigquery) {
  const [datasets] = await bigquery.getDatasets({ autoPaginate: true });
  const hits = [];
  for (const ds of datasets) {
    const id = ds.id;
    if (!id) {
      continue;
    }
    if (await datasetHasIntradayTable(bigquery, id)) {
      hits.push(id);
    }
  }
  return hits;
}

async function resolveGa4BigQueryDatasetId(bigquery) {
  const explicit = process.env.GA4_BIGQUERY_DATASET?.trim();
  if (explicit) {
    const ok = await datasetExists(bigquery, explicit);
    if (!ok) {
      throw new Error(
        `GA4_BIGQUERY_DATASET=${ explicit } not found in BigQuery project (check GOOGLE_CLOUD_PROJECT).`,
      );
    }
    return explicit;
  }

  const prop = process.env.GA4_PROPERTY_ID?.trim();
  if (prop && /^[0-9]+$/.test(prop)) {
    const candidate = `analytics_${ prop }`;
    if (await datasetExists(bigquery, candidate)) {
      return candidate;
    }
  }

  const hits = await listDatasetsWithIntraday(bigquery);
  if (hits.length === 1) {
    return hits[0];
  }
  if (hits.length === 0) {
    throw new Error(
      'No dataset with GA4 intraday tables (events_intraday_*). Enable GA4→BigQuery streaming export, or set GA4_BIGQUERY_DATASET / GA4_PROPERTY_ID.',
    );
  }
  throw new Error(
    `Multiple datasets contain events_intraday_*: ${ hits.join(', ') }. Set GA4_BIGQUERY_DATASET to choose one.`,
  );
}

module.exports = {
  resolveGa4BigQueryDatasetId,
};
