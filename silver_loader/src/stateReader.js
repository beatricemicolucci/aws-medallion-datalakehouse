const { runScalarQuery } = require("./athenaRunner");

/**
 * Determines the last ingestion_date/batch_id already present in
 * the Silver table, by querying Athena directly.
 *
 * _source_ingestion_date is stored as STRING in the Silver table,
 * but contains timestamp values. Therefore it is explicitly cast
 * to TIMESTAMP for temporal comparison.
 *
 * Trade-off: this requires two Athena aggregate queries per
 * incremental run. Actual scan cost depends on the underlying
 * Silver table and Athena/Iceberg query execution.
 */
async function getLastProcessedState(athenaClient, cfg, datasetName) {
  const sql = `
    SELECT MAX(CAST(_source_ingestion_date AS TIMESTAMP)) AS max_date
    FROM ${cfg.silverDatabase}.${datasetName}
  `;

  const maxDate = await runScalarQuery(athenaClient, {
    sql,
    database: cfg.silverDatabase,
    workgroup: cfg.athenaWorkgroup,
    outputLocation: cfg.athenaOutputLocation,
  });

  if (!maxDate) return null;

  const sqlBatch = `
    SELECT MAX(_source_batch_id) AS max_batch
    FROM ${cfg.silverDatabase}.${datasetName}
    WHERE CAST(_source_ingestion_date AS TIMESTAMP) = TIMESTAMP '${maxDate}'
  `;

  const maxBatch = await runScalarQuery(athenaClient, {
    sql: sqlBatch,
    database: cfg.silverDatabase,
    workgroup: cfg.athenaWorkgroup,
    outputLocation: cfg.athenaOutputLocation,
  });

  return {
    ingestionDate: maxDate,
    batchId: maxBatch,
  };
}

module.exports = {
  getLastProcessedState,
};