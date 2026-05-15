# snowstorm

Snowflake onboarding and local credentials for this workspace.

## Prerequisites

- Node.js
- Copy [`.env.sample`](.env.sample) → `.env` and fill **Snowflake** account, service user `LOGIN_NAME`, and **PKCS#8** private key path (see [`.secrets/`](.secrets/) — gitignored).
- Register the matching **public** key on the Snowflake user.

## Onboard

Interactive CLI: list warehouses and databases, optionally create DB/schema, write **`SNOWFLAKE_WAREHOUSE`**, **`SNOWFLAKE_DATABASE`**, **`SNOWFLAKE_SCHEMA`** into `.env`.

```bash
npm install
npm run onboard
```
