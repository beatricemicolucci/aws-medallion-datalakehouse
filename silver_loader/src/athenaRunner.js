
const {
  AthenaClient,
  StartQueryExecutionCommand,
  GetQueryExecutionCommand,
  GetQueryResultsCommand,
} = require("@aws-sdk/client-athena");

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

/**
 * Runs a query on Athena and polls until it completes.
 *
 * On success, logs the amount of data scanned by Athena.
 * This is useful for monitoring query cost, especially for
 * queries that read data from S3.
 *
 * On failure, throws an error containing the exact reason
 * returned by Athena.
 */
async function runQuery(
  athenaClient,
  { sql, database, workgroup, outputLocation }
) {
  const start = await athenaClient.send(
    new StartQueryExecutionCommand({
      QueryString: sql,
      QueryExecutionContext: {
        Database: database,
      },
      WorkGroup: workgroup,
      ResultConfiguration: outputLocation
        ? {
            OutputLocation: outputLocation,
          }
        : undefined,
    })
  );

  const queryExecutionId = start.QueryExecutionId;

  let state = "RUNNING";
  let reason = null;
  let finalStatus = null;

  // Athena is asynchronous: poll periodically until the query
  // reaches a terminal state (SUCCEEDED, FAILED, CANCELLED).
  while (state === "RUNNING" || state === "QUEUED") {
    await sleep(2000);

    finalStatus = await athenaClient.send(
      new GetQueryExecutionCommand({
        QueryExecutionId: queryExecutionId,
      })
    );

    state = finalStatus.QueryExecution.Status.State;
    reason = finalStatus.QueryExecution.Status.StateChangeReason;
  }

  if (state !== "SUCCEEDED") {
    throw new Error(
      `Query failed (${state}) [${queryExecutionId}]: ${reason}`
    );
  }

  // Athena reports the amount of data scanned in bytes.
  // This does not trigger an additional scan or query.
  const scannedBytes =
    finalStatus.QueryExecution.Statistics?.DataScannedInBytes ?? 0;

  const scannedMB = scannedBytes / (1024 * 1024);

  console.log(
    `[Athena] Query ${queryExecutionId} succeeded. Data scanned: ${scannedMB.toFixed(
      2
    )} MB`
  );

  return queryExecutionId;
}

/**
 * Runs a query that returns a single scalar value
 * (e.g. a MAX() aggregate) and returns it as a string.
 *
 * Used to read pipeline state directly from Athena
 * instead of maintaining a separate state file.
 */
async function runScalarQuery(athenaClient, opts) {
  const queryExecutionId = await runQuery(athenaClient, opts);

  const results = await athenaClient.send(
    new GetQueryResultsCommand({
      QueryExecutionId: queryExecutionId,
    })
  );

  // The first row contains the column header.
  // The actual scalar value is in the second row.
  const rows = results.ResultSet.Rows;

  if (rows.length < 2) {
    return null;
  }

  const value = rows[1].Data[0].VarCharValue;

  return value ?? null;
}

module.exports = {
  runQuery,
  runScalarQuery,
};

