const fs = require('fs');
const path = require('path');
const dotenv = require('dotenv');

function loadEnv() {
  dotenv.config({ path: path.join(__dirname, '..', '.env') });
}

function getSnowflakePrivateKeyPem() {
  const inline = process.env.SNOWFLAKE_PRIVATE_KEY;
  if (inline) {
    return inline.replace(/\\n/g, '\n').trim();
  }
  const keyPath = process.env.SNOWFLAKE_PRIVATE_KEY_PATH;
  if (keyPath) {
    return fs.readFileSync(keyPath, 'utf8').trim();
  }
  return null;
}

/**
 * Snowflake key-pair connection (no warehouse / database / schema).
 * Used by onboard before defaults exist in .env.
 */
function getSnowflakeConnectionOptionsMinimal() {
  const account = process.env.SNOWFLAKE_ACCOUNT;
  const username = process.env.SNOWFLAKE_USER;
  const pem = getSnowflakePrivateKeyPem();

  if (!account || !username) {
    throw new Error('Set SNOWFLAKE_ACCOUNT and SNOWFLAKE_USER in .env');
  }
  if (!pem) {
    throw new Error(
      'Set SNOWFLAKE_PRIVATE_KEY_PATH or SNOWFLAKE_PRIVATE_KEY (PKCS#8 PEM private key).',
    );
  }

  const opts = {
    account,
    username,
    authenticator: 'SNOWFLAKE_JWT',
    privateKey: pem,
  };
  if (process.env.SNOWFLAKE_PRIVATE_KEY_PASS) {
    opts.privateKeyPass = process.env.SNOWFLAKE_PRIVATE_KEY_PASS;
  }
  if (process.env.SNOWFLAKE_ROLE) {
    opts.role = process.env.SNOWFLAKE_ROLE;
  }
  return opts;
}

/**
 * Snowflake key-pair + warehouse / database / schema (for sync scripts).
 */
function getSnowflakeConnectionOptionsForSync() {
  const base = getSnowflakeConnectionOptionsMinimal();
  const warehouse = process.env.SNOWFLAKE_WAREHOUSE?.trim();
  const database = process.env.SNOWFLAKE_DATABASE?.trim();
  const schema = process.env.SNOWFLAKE_SCHEMA?.trim();
  if (!warehouse || !database || !schema) {
    throw new Error(
      'Set SNOWFLAKE_WAREHOUSE, SNOWFLAKE_DATABASE, and SNOWFLAKE_SCHEMA in .env (run npm run onboard)',
    );
  }
  return {
    ...base,
    warehouse,
    database,
    schema,
  };
}

/**
 * IANA timezone for GA4 intraday table suffix (YYYYMMDD in property reporting time).
 */
function getGa4ReportingTimezone() {
  return process.env.GA4_REPORTING_TIMEZONE?.trim() || 'UTC';
}

/**
 * Options for @google-cloud/bigquery BigQuery client.
 * Auth: GOOGLE_SERVICE_ACCOUNT_JSON (inline service account JSON) only.
 * projectId: GOOGLE_CLOUD_PROJECT | GCP_PROJECT | BIGQUERY_PROJECT | credentials.project_id
 */
function getBigQueryClientOptions() {
  const projectIdFromEnv =
    process.env.GOOGLE_CLOUD_PROJECT?.trim()
    || process.env.GCP_PROJECT?.trim()
    || process.env.BIGQUERY_PROJECT?.trim()
    || null;

  const jsonRaw = process.env.GOOGLE_SERVICE_ACCOUNT_JSON?.trim();
  if (!jsonRaw) {
    throw new Error('Set GOOGLE_SERVICE_ACCOUNT_JSON in .env');
  }

  let credentials;
  try {
    credentials = JSON.parse(jsonRaw);
  } catch {
    throw new Error('GOOGLE_SERVICE_ACCOUNT_JSON is not valid JSON');
  }

  const projectId =
    projectIdFromEnv
    || credentials?.project_id
    || null;

  if (!projectId) {
    throw new Error(
      'Set GOOGLE_CLOUD_PROJECT (or include project_id in GOOGLE_SERVICE_ACCOUNT_JSON)',
    );
  }

  return {
    projectId,
    credentials,
  };
}

module.exports = {
  loadEnv,
  getSnowflakePrivateKeyPem,
  getSnowflakeConnectionOptionsMinimal,
  getSnowflakeConnectionOptionsForSync,
  getGa4ReportingTimezone,
  getBigQueryClientOptions,
};
