#!/usr/bin/env node
/**
 * Interactive CLI: verify Snowflake key-pair auth, pick warehouse + database (+ schema),
 * optionally create database/schema, write SNOWFLAKE_* to .env
 */
const fs = require('fs');
const path = require('path');
const readline = require('readline/promises');
const snowflake = require('snowflake-sdk');
const {
  loadEnv,
  getSnowflakeConnectionOptionsMinimal,
} = require('./scripts/load-env.js');

const ROOT = __dirname;
const ENV_PATH = path.join(ROOT, '.env');

const SKIP_DB = new Set([
  'SNOWFLAKE',
  'SNOWFLAKE_SAMPLE_DATA',
]);

function pickName(row) {
  return row.name ?? row.NAME ?? row['name'];
}

function isSafeIdentifier(name) {
  return /^[A-Za-z_][A-Za-z0-9_$]*$/.test(name);
}

function quoteId(name) {
  return `"${ String(name).replace(/"/g, '""') }"`;
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

    let whPick = null;
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

    let dbName = null;
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

    let schName = null;
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

    upsertEnv(ENV_PATH, {
      SNOWFLAKE_WAREHOUSE: whPick,
      SNOWFLAKE_DATABASE: dbName,
      SNOWFLAKE_SCHEMA: schName,
    });

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
