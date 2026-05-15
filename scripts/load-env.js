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

module.exports = {
  loadEnv,
  getSnowflakePrivateKeyPem,
  getSnowflakeConnectionOptionsMinimal,
};
