/**
 * Builds the row-level anomaly filter, applied to every date/
 * timestamp column detected in the dataset.
 *
 * A row is EXCLUDED entirely if ANY of its date columns falls
 * outside the configured global bounds.
 *
 * The upper bound is evaluated by Athena at query execution time as
 * the current timestamp plus the configured number of years.
 */
function buildDateAnomalyFilter(dateColumns, dateValidation, alias) {
  if (dateColumns.length === 0) return "";

  const prefix = alias ? `${alias}.` : "";
  const maxTimestamp = `date_add('year', ${dateValidation.maxYearsFromToday}, CURRENT_TIMESTAMP)`;

  const conditions = dateColumns
    .map(
      (c) =>
        `(${prefix}${c.name} IS NOT NULL AND (` +
        `${prefix}${c.name} < TIMESTAMP '${dateValidation.minTimestamp}' OR ` +
        `${prefix}${c.name} > ${maxTimestamp}))`
    )
    .join("\n     OR ");

  return `NOT (
     ${conditions}
)`;
}

/**
 * INITIAL LOAD (dedupe_latest mode):
 * for each PK, keep only the row with the most recent cursor value,
 * tie-broken by ingestion lineage.
 *
 * Date anomalies are excluded.
 * Columns explicitly listed in excludeColumns in config.json are
 * already removed upstream by columnFilter.js.
 */
function buildInitialLoadSQL(cfg, dataset, columns, dateColumns) {
  const { silverDatabase, silverBucket, bronzeDatabase } = cfg;
  const { name, pk, cursor } = dataset;

  const partitionBy = pk.map((c) => `b.${c}`).join(", ");

  const anomalyFilter = buildDateAnomalyFilter(
    dateColumns,
    cfg.dateValidation,
    "b"
  );

  // `columns` is already the FILTERED list:
  // static `excludeColumns` from config.json are applied upstream.
  const selectCols = columns.map((c) => `  ${c.name}`).join(",\n");

  return `
CREATE TABLE ${silverDatabase}.${name}
WITH (
  table_type = 'ICEBERG',
  location = 's3://${silverBucket}/${name}/',
  is_external = false,
  format = 'PARQUET'
)
AS
WITH ranked AS (
  SELECT
    b.*,
    ROW_NUMBER() OVER (
      PARTITION BY ${partitionBy}
      ORDER BY b.${cursor} DESC NULLS LAST,
               CAST(b.ingestion_date AS DATE) DESC,
               b.batch_id DESC
    ) AS rn
  FROM ${bronzeDatabase}.${name} b
  ${anomalyFilter ? `WHERE ${anomalyFilter}` : ""}
)
SELECT
${selectCols},
  ingestion_date AS _source_ingestion_date,
  batch_id       AS _source_batch_id,
  CURRENT_TIMESTAMP AS _silver_loaded_at
FROM ranked
WHERE rn = 1;
`.trim();
}

/**
 * INCREMENTAL UPDATE (dedupe_latest mode):
 * processes only Bronze partitions newer than the last known state,
 * applying the same anomaly filter as the initial load.
 *
 * `ingestion_date` is stored as STRING in Bronze, so it is explicitly
 * cast to DATE for comparison.
 *
 * `batch_id` is stored as STRING and has the following format:
 *
 * YYYYMMDDhhmmssfff_YYYYMMDDhhmmssfff
 *
 * Since the value is a fixed-format chronological identifier,
 * lexicographical comparison preserves chronological ordering.
 *
 * Therefore the complete batch_id is kept and compared as STRING.
 *
 * Idempotence:
 * re-running an already-processed partition is a no-op because
 * WHEN MATCHED requires a strictly greater cursor, while
 * WHEN NOT MATCHED only applies when the PK does not already exist.
 */
function buildMergeSQL(cfg, dataset, columns, dateColumns, sinceState) {
  const { silverDatabase, bronzeDatabase } = cfg;
  const { name, pk, cursor } = dataset;

  const partitionBy = pk.map((c) => `b.${c}`).join(", ");

  const joinCondition = pk
    .map((c) => `target.${c} = source.${c}`)
    .join(" AND ");

  const anomalyFilter = buildDateAnomalyFilter(
    dateColumns,
    cfg.dateValidation,
    "b"
  );

  const updateCols = columns.filter((c) => !pk.includes(c.name));

  const updateSet = updateCols
    .map((c) => `    ${c.name} = source.${c.name}`)
    .concat([
      "    _source_ingestion_date = source.ingestion_date",
      "    _source_batch_id = source.batch_id",
      "    _silver_loaded_at = CURRENT_TIMESTAMP",
    ])
    .join(",\n");

  const insertCols = [
    ...columns.map((c) => c.name),
    "_source_ingestion_date",
    "_source_batch_id",
    "_silver_loaded_at",
  ];

  const insertValues = [
    ...columns.map((c) => `source.${c.name}`),
    "source.ingestion_date",
    "source.batch_id",
    "CURRENT_TIMESTAMP",
  ];

  /*
   * `ingestion_date` is a STRING partition column in Bronze.
   * It is cast to DATE for correct date comparison.
   *
   * `batch_id` is a STRING with a fixed chronological format:
   *
   * YYYYMMDDhhmmssfff_YYYYMMDDhhmmssfff
   *
   * The complete value is preserved and compared lexicographically.
   */
  const lastIngestionDate = sinceState
  ? String(sinceState.ingestionDate).slice(0, 10)
  : null;

  const partitionFilter = sinceState
    ? `(CAST(b.ingestion_date AS DATE) > DATE '${lastIngestionDate}'
      OR (CAST(b.ingestion_date AS DATE) = DATE '${lastIngestionDate}'
          AND b.batch_id > '${sinceState.batchId}'))`
    : "";

  const whereConditions = [partitionFilter, anomalyFilter]
    .filter(Boolean)
    .join("\n   AND ");

  return `
MERGE INTO ${silverDatabase}.${name} AS target
USING (
  SELECT
    b.*,
    ROW_NUMBER() OVER (
      PARTITION BY ${partitionBy}
      ORDER BY b.${cursor} DESC NULLS LAST,
               CAST(b.ingestion_date AS DATE) DESC,
               b.batch_id DESC
    ) AS rn
  FROM ${bronzeDatabase}.${name} b
  ${whereConditions ? `WHERE ${whereConditions}` : ""}
) AS source
ON ${joinCondition} AND source.rn = 1
WHEN MATCHED AND source.${cursor} > target.${cursor} THEN
  UPDATE SET
${updateSet}
WHEN NOT MATCHED THEN
  INSERT (${insertCols.join(", ")})
  VALUES (${insertValues.join(", ")})
`.trim();
}

/**
 * APPEND-ONLY mode (ORDCLI_R case):
 * no cursor-based deduplication, since the PK already includes
 * the cursor and each row is a distinct event.
 *
 * The date anomaly filter and the statically configured column
 * exclusions are still applied.
 */
function buildAppendOnlyInitialLoadSQL(
  cfg,
  dataset,
  columns,
  dateColumns
) {
  const { silverDatabase, silverBucket, bronzeDatabase } = cfg;
  const { name } = dataset;

  const selectCols = columns
    .map((c) => `  b.${c.name}`)
    .join(",\n");

  const anomalyFilter = buildDateAnomalyFilter(
    dateColumns,
    cfg.dateValidation,
    "b"
  );

  return `
CREATE TABLE ${silverDatabase}.${name}
WITH (
  table_type = 'ICEBERG',
  location = 's3://${silverBucket}/${name}/',
  is_external = false,
  format = 'PARQUET'
)
AS
SELECT
${selectCols},
  b.ingestion_date AS _source_ingestion_date,
  b.batch_id       AS _source_batch_id,
  CURRENT_TIMESTAMP AS _silver_loaded_at
FROM ${bronzeDatabase}.${name} b
${anomalyFilter ? `WHERE ${anomalyFilter}` : ""};
`.trim();
}

/**
 * Incremental append for "append_only" datasets:
 * plain INSERT of new partitions, no merge/update logic,
 * with the same anomaly filter applied.
 *
 * `ingestion_date` is cast to DATE.
 * `batch_id` is compared as the complete STRING value.
 */
function buildAppendOnlyIncrementalSQL(
  cfg,
  dataset,
  columns,
  dateColumns,
  sinceState
) {
  const { silverDatabase, bronzeDatabase } = cfg;
  const { name } = dataset;

  const selectCols = columns
    .map((c) => `  b.${c.name}`)
    .join(",\n");

  const anomalyFilter = buildDateAnomalyFilter(
    dateColumns,
    cfg.dateValidation,
    "b"
  );

  const lastIngestionDate = sinceState
  ? String(sinceState.ingestionDate).slice(0, 10)
  : null;

  const partitionFilter = sinceState
    ? `(CAST(b.ingestion_date AS DATE) > DATE '${lastIngestionDate}'
      OR (CAST(b.ingestion_date AS DATE) = DATE '${lastIngestionDate}'
          AND b.batch_id > '${sinceState.batchId}'))`
    : "";

  const whereConditions = [partitionFilter, anomalyFilter]
    .filter(Boolean)
    .join("\n   AND ");

  return `
INSERT INTO ${silverDatabase}.${name}
SELECT
${selectCols},
  b.ingestion_date AS _source_ingestion_date,
  b.batch_id       AS _source_batch_id,
  CURRENT_TIMESTAMP AS _silver_loaded_at
FROM ${bronzeDatabase}.${name} b
${whereConditions ? `WHERE ${whereConditions}` : ""};
`.trim();
}

module.exports = {
  buildDateAnomalyFilter,
  buildInitialLoadSQL,
  buildMergeSQL,
  buildAppendOnlyInitialLoadSQL,
  buildAppendOnlyIncrementalSQL,
};
