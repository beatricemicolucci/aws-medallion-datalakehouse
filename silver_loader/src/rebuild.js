const { runQuery } = require("./athenaRunner");
const { S3Client, ListObjectsV2Command, DeleteObjectsCommand } = require("@aws-sdk/client-s3");

/**
 * Rebuilds a Silver table from scratch: drops the existing Iceberg
 * table (Glue Data Catalog metadata) and deletes the underlying S3
 * data files, then the caller re-runs the initial-load query to
 * recreate it cleanly.
 *
 * Why this is safe: Bronze is never touched, is append-only, and
 * retains full history. A "dirty" Silver table (loaded before the
 * quality filters existed) can always be reconstructed from Bronze
 * at near-zero risk — this is exactly the advantage of keeping
 * Bronze immutable, discussed earlier in the project.
 *
 * DROP TABLE alone is not enough for a managed Iceberg table backed
 * by CTAS: Athena's DROP TABLE removes only the Data Catalog entry,
 * not the physical Parquet/Iceberg files. Without an explicit S3
 * cleanup, the next CREATE TABLE at the same location would either
 * fail or mix old and new data files. Hence the explicit S3 delete.
 */
async function rebuildTable(athenaClient, s3Client, cfg, datasetName) {
  console.log(`  Rebuilding ${datasetName}: dropping existing table...`);

  await runQuery(athenaClient, {
    sql: `DROP TABLE IF EXISTS ${cfg.silverDatabase}.${datasetName}`,
    database: cfg.silverDatabase,
    workgroup: cfg.athenaWorkgroup,
    outputLocation: cfg.athenaOutputLocation,
  });

  const prefix = `${datasetName}/`;
  console.log(`  Deleting existing S3 data under s3://${cfg.silverBucket}/${prefix} ...`);
  await deleteAllObjectsUnderPrefix(s3Client, cfg.silverBucket, prefix);

  console.log(`  Rebuild cleanup complete. Ready for a fresh initial load.`);
}

/**
 * Deletes every object under a given S3 prefix, handling pagination
 * and batching deletes in groups of up to 1000 (S3 API limit).
 */
async function deleteAllObjectsUnderPrefix(s3Client, bucket, prefix) {
  let continuationToken;
  let totalDeleted = 0;

  do {
    const listResult = await s3Client.send(
      new ListObjectsV2Command({
        Bucket: bucket,
        Prefix: prefix,
        ContinuationToken: continuationToken,
      })
    );

    const objects = listResult.Contents || [];
    if (objects.length > 0) {
      const deleteResult = await s3Client.send(
        new DeleteObjectsCommand({
          Bucket: bucket,
          Delete: { Objects: objects.map((o) => ({ Key: o.Key })) },
        })
      );

      // DeleteObjectsCommand does NOT throw on partial failures: a
      // failed individual object delete shows up in the `Errors`
      // array of a successful response, not as a thrown exception.
      // Silently ignoring this would leave orphaned data files that
      // could corrupt the next rebuild (mixed old/new data at the
      // same S3 location). Fail loudly instead.
      if (deleteResult.Errors && deleteResult.Errors.length > 0) {
        const details = deleteResult.Errors.map((e) => `${e.Key}: ${e.Code} — ${e.Message}`).join("; ");
        throw new Error(
          `Failed to delete ${deleteResult.Errors.length} object(s) under s3://${bucket}/${prefix}: ${details}`
        );
      }

      totalDeleted += objects.length;
    }

    continuationToken = listResult.IsTruncated ? listResult.NextContinuationToken : undefined;
  } while (continuationToken);

  console.log(`  Deleted ${totalDeleted} object(s) from S3.`);
}

module.exports = { rebuildTable };