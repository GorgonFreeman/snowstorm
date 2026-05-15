#!/usr/bin/env node
/**
 * Interactive CLI: verify Snowflake key-pair auth, pick warehouse + database (+ schema),
 * optionally create database/schema, create GA4 landing tables + file format, write .env.
 * Pre-set SNOWFLAKE_* / GA4_* in .env are used when they match Snowflake (warehouse, database, schema)
 * or as defaults for GA4 prompts (Enter keeps .env).
 */
const fs = require('fs');
const path = require('path');
const readline = require('readline/promises');
const snowflake = require('snowflake-sdk');
const {
  loadEnv,
  getSnowflakeConnectionOptionsMinimal,
} = require('./scripts/load-env.js');
const { quoteIdent } = require('./scripts/snowflake-sql.js');

const ROOT = __dirname;
const ENV_PATH = path.join(ROOT, '.env');

const SKIP_DB = new Set([
  'SNOWFLAKE',
  'SNOWFLAKE_SAMPLE_DATA',
]);

/** Resolve env value to the canonical string from `items` (Snowflake uppercases identifiers). */
function pickCanonicalIfListed(items, envRaw) {
  const v = envRaw?.trim();
  if (!v || !items?.length) {
    return null;
  }
  const upper = v.toUpperCase();
  for (const item of items) {
    if (item && item.toUpperCase() === upper) {
      return item;
    }
  }
  return null;
}

function pickName(row) {
  return row.name ?? row.NAME ?? row['name'];
}

function isSafeIdentifier(name) {
  return /^[A-Za-z_][A-Za-z0-9_$]*$/.test(name);
}

function quoteId(name) {
  return quoteIdent(name);
}

function upsertEnv(envPath, updates) {
  const keys = Object.keys(updates);
  let text = '';
  try {
    text = fs.readFileSync(envPath, 'utf8');
  } catch (_) {
    text = '';
  }
  const lines = text.split(/\r?\n/);
  const replaced = new Set();
  const out = lines.map((line) => {
    const m = line.match(/^([A-Za-z_][A-Za-z0-9_]*)=(.*)$/);
    if (m && Object.prototype.hasOwnProperty.call(updates, m[1])) {
      replaced.add(m[1]);
      return `${ m[1] }=${ updates[m[1]] }`;
    }
    return line;
  });
  for (const k of keys) {
    if (!replaced.has(k)) {
      out.push(`${ k }=${ updates[k] }`);
    }
  }
  fs.writeFileSync(envPath, out.join('\n').replace(/\n+$/, '\n'), 'utf8');
}

function runQuery(conn, sqlText) {
  return new Promise((resolve, reject) => {
    conn.execute({
      sqlText,
      complete: (err, _stmt, rows) => {
        if (err) {
          reject(err);
          return;
        }
        resolve(rows || []);
      },
    });
  });
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

async function pickFromList(rl, title, items, labels, createLabel) {
  console.log(`\n${ title }`);
  items.forEach((item, i) => {
    console.log(`  ${ i + 1 }. ${ labels(item, i) }`);
  });
  console.log(`  ${ items.length + 1 }. ${ createLabel }`);
  const raw = await rl.question(`\nEnter 1–${ items.length + 1 }: `);
  const n = parseInt(raw.trim(), 10);
  if (raw.trim() === `${ items.length + 1 }` || n === items.length + 1) {
    return { create: true };
  }
  if (!Number.isFinite(n) || n < 1 || n > items.length) {
    return null;
  }
  return { create: false, item: items[n - 1] };
}

async function main() {
  loadEnv();
  try {
    snowflake.configure({ logLevel: 'ERROR' });
  } catch (_) {
    /* older SDK */
  }

  const rl = readline.createInterface({
    input: process.stdin,
    output: process.stdout,
  });

  let conn;
  try {
    const baseOpts = getSnowflakeConnectionOptionsMinimal();
    console.log('Connecting to Snowflake…');
    conn = await connect(baseOpts);
    await runQuery(conn, 'SELECT 1 AS ok');
    console.log('Snowflake login OK.');

    const whRows = await runQuery(conn, 'SHOW WAREHOUSES');
    const warehouses = whRows
      .map((r) => pickName(r))
      .filter(Boolean)
      .filter((n) => !/^SYSTEM\$/.test(n));

    if (!warehouses.length) {
      console.error('No warehouses visible.');
      process.exit(1);
    }

    let whPick = pickCanonicalIfListed(warehouses, process.env.SNOWFLAKE_WAREHOUSE);
    if (whPick) {
      console.log(`\nUsing SNOWFLAKE_WAREHOUSE from .env: ${ whPick }`);
    } else if (process.env.SNOWFLAKE_WAREHOUSE?.trim()) {
      console.log(
        `\nNote: SNOWFLAKE_WAREHOUSE is set but not in the visible list; pick a warehouse below.`,
      );
    }
    while (!whPick) {
      const picked = await pickFromList(
        rl,
        'Warehouses',
        warehouses,
        (name) => `${ name }`,
        'Other (type a warehouse name)',
      );
      if (!picked) {
        console.log('Invalid choice, try again.');
        continue;
      }
      if (picked.create) {
        const name = (await rl.question('Warehouse name: ')).trim();
        if (!name || !isSafeIdentifier(name)) {
          console.log('Use letters, numbers, underscore; start with letter or _.');
          continue;
        }
        whPick = name;
      } else {
        whPick = picked.item;
      }
    }

    await runQuery(conn, `USE WAREHOUSE ${ quoteId(whPick) }`);

    const dbRows = await runQuery(conn, 'SHOW DATABASES');
    let databases = dbRows
      .map((r) => pickName(r))
      .filter(Boolean)
      .filter((n) => !SKIP_DB.has(n));

    let dbName = pickCanonicalIfListed(databases, process.env.SNOWFLAKE_DATABASE);
    if (dbName) {
      console.log(`\nUsing SNOWFLAKE_DATABASE from .env: ${ dbName }`);
    } else if (process.env.SNOWFLAKE_DATABASE?.trim()) {
      console.log(
        `\nNote: SNOWFLAKE_DATABASE is set but not in the visible list; pick or create below.`,
      );
    }
    while (!dbName) {
      const picked = await pickFromList(
        rl,
        'Databases (Snowflake-managed ones hidden)',
        databases,
        (name) => name,
        'Create a new database',
      );
      if (!picked) {
        console.log('Invalid choice, try again.');
        continue;
      }
      if (picked.create) {
        const name = (await rl.question('New database name: ')).trim();
        if (!name || !isSafeIdentifier(name)) {
          console.log('Invalid name.');
          continue;
        }
        await runQuery(
          conn,
          `CREATE DATABASE IF NOT EXISTS ${ quoteId(name) } COMMENT = 'snowstorm GA4 connector'`,
        );
        dbName = name.toUpperCase();
        databases = [...databases, dbName];
      } else {
        dbName = picked.item;
      }
    }

    await runQuery(conn, `USE DATABASE ${ quoteId(dbName) }`);

    const schRows = await runQuery(conn, `SHOW SCHEMAS IN DATABASE ${ quoteId(dbName) }`);
    const schemas = schRows
      .map((r) => pickName(r))
      .filter(Boolean)
      .filter((n) => n !== 'INFORMATION_SCHEMA');

    let schName = pickCanonicalIfListed(schemas, process.env.SNOWFLAKE_SCHEMA);
    if (schName) {
      console.log(`\nUsing SNOWFLAKE_SCHEMA from .env: ${ schName }`);
    } else if (process.env.SNOWFLAKE_SCHEMA?.trim()) {
      console.log(
        `\nNote: SNOWFLAKE_SCHEMA is set but not in this database; pick or create below.`,
      );
    }
    while (!schName) {
      const picked = await pickFromList(
        rl,
        `Schemas in ${ dbName }`,
        schemas,
        (name) => name,
        'Create a new schema',
      );
      if (!picked) {
        console.log('Invalid choice, try again.');
        continue;
      }
      if (picked.create) {
        const name = (await rl.question('New schema name: ')).trim();
        if (!name || !isSafeIdentifier(name)) {
          console.log('Invalid name.');
          continue;
        }
        await runQuery(
          conn,
          `CREATE SCHEMA IF NOT EXISTS ${ quoteId(dbName) }.${ quoteId(name) } COMMENT = 'snowstorm GA4 landing'`,
        );
        schName = name.toUpperCase();
      } else {
        schName = picked.item;
      }
    }

    await runQuery(conn, `USE SCHEMA ${ quoteId(dbName) }.${ quoteId(schName) }`);

    await runQuery(
      conn,
      `CREATE FILE FORMAT IF NOT EXISTS ${ quoteId('SNOWSTORM_NDJSON_FMT') } TYPE = 'JSON'`,
    );

    await runQuery(
      conn,
      `CREATE TABLE IF NOT EXISTS ${ quoteId('GA4_INTRADAY_SYNC_STATE') } (
  sync_key VARCHAR(512) PRIMARY KEY,
  watermark_event_timestamp BIGINT NOT NULL,
  updated_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
) COMMENT = 'snowstorm: high-water mark per intraday BQ partition'`,
    );

    await runQuery(
      conn,
      `CREATE TABLE IF NOT EXISTS ${ quoteId('GA4_INTRADAY_EVENTS') } (
  raw VARIANT NOT NULL,
  bq_table_date VARCHAR(8) NOT NULL,
  loaded_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
) COMMENT = 'snowstorm: GA4 events_intraday rows merged from BigQuery'`,
    );

    upsertEnv(ENV_PATH, {
      SNOWFLAKE_WAREHOUSE: whPick,
      SNOWFLAKE_DATABASE: dbName,
      SNOWFLAKE_SCHEMA: schName,
    });

    if (process.env.GOOGLE_SERVICE_ACCOUNT_JSON?.trim()) {
      try {
        const { BigQuery } = require('@google-cloud/bigquery');
        const { getBigQueryClientOptions } = require('./scripts/load-env.js');
        const { resolveGa4BigQueryDatasetId } = require('./scripts/resolve-ga4-bq-dataset.js');
        const bq = new BigQuery(getBigQueryClientOptions());
        const id = await resolveGa4BigQueryDatasetId(bq);
        upsertEnv(ENV_PATH, { GA4_BIGQUERY_DATASET: id });
        console.log(`\nGA4_BIGQUERY_DATASET (BigQuery API): ${ id }`);
      } catch (e) {
        console.warn(`\n${ e.message }`);
        const manual = (
          await rl.question('Enter GA4 BigQuery dataset id manually (e.g. analytics_402247571):\n> ')
        ).trim();
        if (manual && isSafeIdentifier(manual)) {
          upsertEnv(ENV_PATH, { GA4_BIGQUERY_DATASET: manual });
          console.log(`  Wrote GA4_BIGQUERY_DATASET=${ manual }`);
        }
      }
    } else {
      console.log('\nSkipping GA4 dataset API resolution (add GOOGLE_SERVICE_ACCOUNT_JSON to .env).');
      const manual = (
        await rl.question('GA4 BigQuery dataset id (optional, Enter to skip):\n> ')
      ).trim();
      if (manual && isSafeIdentifier(manual)) {
        upsertEnv(ENV_PATH, { GA4_BIGQUERY_DATASET: manual });
        console.log(`  Wrote GA4_BIGQUERY_DATASET=${ manual }`);
      }
    }

    const tzFromEnv = process.env.GA4_REPORTING_TIMEZONE?.trim();
    const tzPrompt = await rl.question(
      `GA4 reporting timezone (IANA). Enter = ${ tzFromEnv ? `keep ${ tzFromEnv }` : 'use UTC' }:\n> `,
    );
    const tz = tzPrompt.trim() || tzFromEnv || 'UTC';
    upsertEnv(ENV_PATH, { GA4_REPORTING_TIMEZONE: tz });
    if (tzPrompt.trim()) {
      console.log(`  Wrote GA4_REPORTING_TIMEZONE=${ tz }`);
    } else {
      console.log(`  Using GA4_REPORTING_TIMEZONE=${ tz }`);
    }

    console.log(
      '\nOne-time Snowflake objects (if not already present):',
      `\n  FILE FORMAT ${ schName }.SNOWSTORM_NDJSON_FMT`,
      `\n  TABLE ${ schName }.GA4_INTRADAY_SYNC_STATE`,
      `\n  TABLE ${ schName }.GA4_INTRADAY_EVENTS`,
    );
    console.log(
      `\nWrote to ${ path.relative(process.cwd(), ENV_PATH) || '.env' }:`,
      `\n  SNOWFLAKE_WAREHOUSE=${ whPick }`,
      `\n  SNOWFLAKE_DATABASE=${ dbName }`,
      `\n  SNOWFLAKE_SCHEMA=${ schName }`,
    );
  } catch (err) {
    console.error('onboard failed', err.message || err);
    process.exitCode = 1;
  } finally {
    if (conn) {
      conn.destroy(() => {});
    }
    await rl.close();
  }
}

main();
