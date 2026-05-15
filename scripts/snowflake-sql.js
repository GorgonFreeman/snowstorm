/**
 * Safe double-quoted Snowflake identifier (Unicode letters also ok in SF; keep ASCII strict).
 */
function quoteIdent(name) {
  return `"${ String(name).replace(/"/g, '""') }"`;
}

module.exports = {
  quoteIdent,
};
