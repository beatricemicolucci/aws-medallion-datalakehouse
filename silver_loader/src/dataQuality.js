const { runQuery } = require("./athenaRunner");
const { GetQueryResultsCommand } = require("@aws-sdk/client-athena");

/**
 * Builds a single query that, in one Bronze table scan, computes
 * the non-null count of every business column. One row is returned,
 * with one aggregate column per input column plus a total row count.
 *
 * Why a single combined query instead of one query per column:
 * Athena bills by data scanned. Scanning the same table once for
 * all columns is far cheaper than N separate full scans.
 */
function buildNullCheckSQL(bronzeDatabase, datasetName, columns) {
  const countExprs = columns.map((c) => `  COUNT(${c.name}) AS ${c.name}__nonnull`).join(",\n");

  return `
SELECT
  COUNT(*) AS total_rows,
${countExprs}
FROM ${bronzeDatabase}.${datasetName}
`.trim();
}

/**
 * Runs the null-check query and returns { totalRows, nonNullCounts }
 * where nonNullCounts is a map columnName -> number of non-null rows.
 */
async function runNullCheck(athenaClient, cfg, datasetName, columns) {
  const sql = buildNullCheckSQL(cfg.bronzeDatabase, datasetName, columns);

  const queryExecutionId = await runQuery(athenaClient, {
    sql,
    database: cfg.bronzeDatabase,
    workgroup: cfg.athenaWorkgroup,
    outputLocation: cfg.athenaOutputLocation,
  });

  const results = await athenaClient.send(
    new GetQueryResultsCommand({ QueryExecutionId: queryExecutionId })
  );

  const headerRow = results.ResultSet.Rows[0].Data.map((d) => d.VarCharValue);
  const valueRow = results.ResultSet.Rows[1].Data.map((d) => d.VarCharValue);

  const record = {};
  headerRow.forEach((col, i) => {
    record[col] = valueRow[i];
  });

  const totalRows = parseInt(record.total_rows, 10);
  const nonNullCounts = {};
  for (const c of columns) {
    nonNullCounts[c.name] = parseInt(record[`${c.name}__nonnull`], 10);
  }

  return { totalRows, nonNullCounts };
}

/**
 * Given the null-check result, returns only the columns that have
 * AT LEAST one non-null value in the current Bronze data. Columns
 * that are 100% NULL right now are excluded from this run's schema.
 *
 * Important: this check is re-evaluated on EVERY run against live
 * Bronze data. If a column that used to be all-NULL starts getting
 * populated, it will automatically be included again in a future
 * run (see schemaEvolution.js for how the Silver table schema
 * catches up via ALTER TABLE ADD COLUMN).
 */
function pruneFullNullColumns(columns, nullCheckResult) {
  return columns.filter((c) => nullCheckResult.nonNullCounts[c.name] > 0);
}

module.exports = { buildNullCheckSQL, runNullCheck, pruneFullNullColumns };
