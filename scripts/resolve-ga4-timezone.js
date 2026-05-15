/**
 * GA4 property reporting timezone via Analytics Admin API (v1beta getProperty).
 * Override: set GA4_REPORTING_TIMEZONE in .env to skip the API.
 *
 * Requires GOOGLE_SERVICE_ACCOUNT_JSON and GA4_PROPERTY_ID when not overriding.
 * The service account must have access to the GA4 property (e.g. Viewer in Admin).
 */
const { v1beta } = require('@google-analytics/admin');

function getCredentialsFromEnv() {
  const jsonRaw = process.env.GOOGLE_SERVICE_ACCOUNT_JSON?.trim();
  if (!jsonRaw) {
    throw new Error('Set GOOGLE_SERVICE_ACCOUNT_JSON');
  }
  try {
    return JSON.parse(jsonRaw);
  } catch {
    throw new Error('GOOGLE_SERVICE_ACCOUNT_JSON is not valid JSON');
  }
}

async function resolveGa4ReportingTimezone() {
  const fromEnv = process.env.GA4_REPORTING_TIMEZONE?.trim();
  if (fromEnv) {
    return fromEnv;
  }

  const prop = process.env.GA4_PROPERTY_ID?.trim();
  if (!prop || !/^[0-9]+$/.test(prop)) {
    return 'UTC';
  }

  const credentials = getCredentialsFromEnv();
  const client = new v1beta.AnalyticsAdminServiceClient({ credentials });
  const [property] = await client.getProperty({
    name: `properties/${ prop }`,
  });

  const tz = property?.timeZone?.trim();
  if (!tz) {
    throw new Error(
      'Analytics Admin API returned no timeZone; check GA4_PROPERTY_ID and service account access to this property.',
    );
  }
  return tz;
}

module.exports = {
  resolveGa4ReportingTimezone,
};
