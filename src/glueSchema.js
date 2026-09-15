const { GlueClient, GetTableCommand, GetPartitionsCommand } = require("@aws-sdk/client-glue");

const PARTITION_COLUMNS = new Set(["ingestion_date", "batch_id"]);

/**
 * Fetches the full column list (name + Glue/Hive type) of a Bronze
 * table, excluding partition columns (ingestion_date, batch_id).
 * Reading types (not just names) is what allows automatic detection
 * of which columns are dates/timestamps, instead of maintaining a
 * hand-written list per dataset.
 */
async function getColumnsWithTypes(glueClient, database, tableName) {
  const cmd = new GetTableCommand({ DatabaseName: database, Name: tableName });
  const result = await glueClient.send(cmd);

  return result.Table.StorageDescriptor.Columns.filter((c) => !PARTITION_COLUMNS.has(c.Name)).map(
    (c) => ({ name: c.Name, type: c.Type })
  );
}

/**
 * Returns true if a Glue/Hive column type represents a date or
 * timestamp. Used to auto-detect which columns must be checked
 * against the anomaly bounds (min/max timestamp), without a
 * manually curated list per dataset.
 */
function isDateType(hiveType) {
  const t = hiveType.toLowerCase();
  return t.startsWith("timestamp") || t.startsWith("date");
}

/**
 * Checks whether a table already exists in the Glue Data Catalog.
 * Used to decide between initial load (CTAS) and incremental
 * update (MERGE), and to detect schema evolution needs.
 */
async function tableExists(glueClient, database, tableName) {
  try {
    await glueClient.send(new GetTableCommand({ DatabaseName: database, Name: tableName }));
    return true;
  } catch (err) {
    if (err.name === "EntityNotFoundException") return false;
    throw err;
  }
}

/**
 * Returns the current column set of an EXISTING table (name + type).
 * Used for schema-evolution comparison: which columns does the
 * Silver table currently have vs. which ones Bronze now offers.
 */
async function getExistingColumns(glueClient, database, tableName) {
  return getColumnsWithTypes(glueClient, database, tableName);
}

/**
 * Lists all ingestion_date/batch_id partitions currently present in
 * a Bronze table. Not used in the main load path today, kept as a
 * utility for debugging/reporting.
 */
async function listBronzePartitions(glueClient, database, tableName) {
  const partitions = [];
  let nextToken;

  do {
    const cmd = new GetPartitionsCommand({
      DatabaseName: database,
      TableName: tableName,
      NextToken: nextToken,
    });
    const result = await glueClient.send(cmd);

    for (const p of result.Partitions) {
      const [ingestionDate, batchId] = p.Values;
      partitions.push({ ingestionDate, batchId });
    }
    nextToken = result.NextToken;
  } while (nextToken);

  return partitions;
}

module.exports = {
  getColumnsWithTypes,
  getExistingColumns,
  isDateType,
  tableExists,
  listBronzePartitions,
};
