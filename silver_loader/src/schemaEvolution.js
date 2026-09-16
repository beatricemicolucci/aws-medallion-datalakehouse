const { runQuery } = require("./athenaRunner");

/**
 * Compares the currently retained Bronze columns against the
 * columns already present in Silver, and returns the ones that
 * are missing from Silver.
 *
 * Columns listed in excludeColumns are not part of retainedColumns
 * and therefore are not added to Silver.
 *
 * Design choice: columns are only ever ADDED, never DROPPED, once
 * a Silver table exists. Dropping a column that Silver already has
 * would be a destructive, backward-incompatible change.
 */
function findMissingColumns(retainedColumns, existingSilverColumns) {
  const existingNames = new Set(existingSilverColumns.map((c) => c.name));
  return retainedColumns.filter((c) => !existingNames.has(c.name));
}

/**
 * Issues ALTER TABLE ... ADD COLUMNS for any column found missing.
 * This is a metadata-only operation in Iceberg: existing data files
 * are not rewritten, and historical rows simply read as NULL for
 * the newly added column (which is correct: they never had a value
 * for it in the first place).
 */
async function addMissingColumns(athenaClient, cfg, datasetName, missingColumns) {
  if (missingColumns.length === 0) return;

  const columnDefs = missingColumns.map((c) => `${c.name} ${c.type}`).join(", ");
  const sql = `ALTER TABLE ${cfg.silverDatabase}.${datasetName} ADD COLUMNS (${columnDefs})`;

  console.log(`  Schema evolution: adding column(s) ${missingColumns.map((c) => c.name).join(", ")}`);

  await runQuery(athenaClient, {
    sql,
    database: cfg.silverDatabase,
    workgroup: cfg.athenaWorkgroup,
    outputLocation: cfg.athenaOutputLocation,
  });
}

module.exports = { findMissingColumns, addMissingColumns };
