const { GlueClient } = require("@aws-sdk/client-glue");
const { AthenaClient } = require("@aws-sdk/client-athena");
const { S3Client } = require("@aws-sdk/client-s3");

const config = require("../config.json");
const { getColumnsWithTypes, getExistingColumns, isDateType, tableExists } = require("./glueSchema");
const { applyExcludeColumns } = require("./columnFilter");
const { runQuery } = require("./athenaRunner");
const { getLastProcessedState } = require("./stateReader");
const { findMissingColumns, addMissingColumns } = require("./schemaEvolution");
const { rebuildTable } = require("./rebuild");
const {
  buildInitialLoadSQL,
  buildMergeSQL,
  buildAppendOnlyInitialLoadSQL,
  buildAppendOnlyIncrementalSQL,
} = require("./queryBuilder");

// ── CLI arguments ────────────────────────────────────────────
const args = process.argv.slice(2);
const onlyDataset = args.find((a) => a.startsWith("--dataset="))?.split("=")[1];
const confirmAppendOnly = args.includes("--confirm-append-only");
const dryRun = args.includes("--dry-run");
const forceRebuild = args.includes("--rebuild");

async function main() {
  const glueClient = new GlueClient({ region: config.region });
  const athenaClient = new AthenaClient({ region: config.region });
  const s3Client = new S3Client({ region: config.region });

  const datasetsToProcess = onlyDataset
    ? config.datasets.filter((d) => d.name === onlyDataset)
    : config.datasets;

  if (datasetsToProcess.length === 0) {
    console.error(`No dataset found with name "${onlyDataset}" in config.json`);
    process.exit(1);
  }

  console.log(`=== Silver Loader — ${datasetsToProcess.length} dataset(s) to process ===`);
  if (forceRebuild) {
    console.log(
      dryRun
        ? "Mode: REBUILD --dry-run (simulated: no DROP TABLE, no S3 deletion — only the resulting query is shown)"
        : "Mode: REBUILD (existing Silver tables will be dropped and recreated)"
    );
  }

  const summary = [];
  let hasErrors = false;

  for (const dataset of datasetsToProcess) {
    console.log(`\n──────────────────────────────────────────`);
    console.log(`Dataset: ${dataset.name}  (mode: ${dataset.mode})`);
    console.log(`──────────────────────────────────────────`);

    if (dataset.requiresConfirmation && !confirmAppendOnly) {
      console.log(
        `⏭️  SKIPPED: this dataset requires explicit confirmation.\n` +
          `   Reason: ${dataset.note}\n` +
          `   To run it anyway: add the --confirm-append-only flag`
      );
      summary.push({ dataset: dataset.name, status: "SKIPPED (requires confirmation)" });
      continue;
    }

    try {
      // Step 1: read the real Bronze schema (name + type) from Glue.
      // This is a metadata-only call (GetTable) — NOT an Athena scan.
      console.log("Reading Bronze schema from Glue Data Catalog...");
      const allColumns = await getColumnsWithTypes(glueClient, config.bronzeDatabase, dataset.name);

      // Step 2: auto-detect which columns are dates/timestamps,
      // purely from their Glue type — no manual list to maintain.
      // (Unrelated to excludeColumns: date detection stays dynamic,
      // only the NULL-column exclusion became static per requirement.)
      const dateColumns = allColumns.filter((c) => isDateType(c.type));
      console.log(`  → ${allColumns.length} columns total, ${dateColumns.length} detected as date/timestamp`);

      // Step 3: apply the STATIC excludeColumns list from config.json.
      // No Athena scan here — this is a pure in-memory filter against
      // the schema already fetched in Step 1, validated for typos.
      const excludeColumns = dataset.excludeColumns || [];
      const retainedColumns = applyExcludeColumns(allColumns, excludeColumns, dataset.name);
      console.log(
        `  → ${retainedColumns.length} columns retained, ${excludeColumns.length} excluded via config`
      );

      // Step 4: does the Silver table already exist?
      let exists = await tableExists(glueClient, config.silverDatabase, dataset.name);

      // Step 5: rebuild handling.
      // Real run: drop the table + delete its S3 data, then treat
      // this as a fresh initial load.
      // Dry run: perform NO destructive action at all (no DROP
      // TABLE, no S3 deletion), but still simulate "table does not
      // exist" so the INITIAL LOAD query (the one a real rebuild
      // would actually run) is generated and shown, instead of an
      // incorrect INCREMENTAL query.
      if (exists && forceRebuild) {
        if (dryRun) {
          console.log("DRY RUN: simulating rebuild — no destructive action taken.");
          exists = false;
        } else {
          await rebuildTable(athenaClient, s3Client, config, dataset.name);
          exists = false;
        }
      }

      let sql;
      let operation;

      if (!exists) {
        operation = "INITIAL LOAD";
        sql =
          dataset.mode === "append_only"
            ? buildAppendOnlyInitialLoadSQL(config, dataset, retainedColumns, dateColumns)
            : buildInitialLoadSQL(config, dataset, retainedColumns, dateColumns);
      } else {
        // Step 5b: schema evolution — add any column that is present
        // in Bronze/retainedColumns but missing from the existing
        // Silver table. This is a Glue GetTable metadata call, not
        // an Athena scan. Columns are only ever ADDED, never dropped.
        console.log("Checking for schema evolution (columns present in Bronze but missing in Silver)...");
        const existingSilverColumns = await getExistingColumns(glueClient, config.silverDatabase, dataset.name);
        const missingColumns = findMissingColumns(retainedColumns, existingSilverColumns);

        if (missingColumns.length > 0) {
          console.log(`  → ${missingColumns.length} column(s) need to be added: ${missingColumns.map((c) => c.name).join(", ")}`);
          if (!dryRun) {
            await addMissingColumns(athenaClient, config, dataset.name, missingColumns);
          } else {
            console.log("  (dry run: ALTER TABLE not executed)");
          }
        } else {
          console.log("  → No schema changes needed.");
        }

        // This is a real Athena scan against Silver (see cost note
        // in stateReader.js). It is strictly necessary to build a
        // correct incremental query, including in dry-run mode, so
        // the preview reflects the query a real run would execute.
        console.log("Existing Silver table: determining last processed batch (Athena query)...");
        const lastState = await getLastProcessedState(athenaClient, config, dataset.name);
        if (lastState) {
          console.log(`  → Last batch in Silver: ${lastState.ingestionDate} / batch ${lastState.batchId}`);
        } else {
          console.log("  → No data found in Silver yet (empty table?)");
        }

        operation = "INCREMENTAL UPDATE";
        sql =
          dataset.mode === "append_only"
            ? buildAppendOnlyIncrementalSQL(config, dataset, retainedColumns, dateColumns, lastState)
            : buildMergeSQL(config, dataset, retainedColumns, dateColumns, lastState);
      }

      console.log(`Operation: ${operation}`);

      if (dryRun) {
        console.log("\n--- DRY RUN: generated query (NOT executed) ---");
        console.log(sql);
        summary.push({ dataset: dataset.name, status: `DRY RUN (${operation})` });
        continue;
      }

      console.log("Executing query on Athena...");
      const queryId = await runQuery(athenaClient, {
        sql,
        database: config.silverDatabase,
        workgroup: config.athenaWorkgroup,
        outputLocation: config.athenaOutputLocation,
      });

      console.log(`✅ Done (QueryExecutionId: ${queryId})`);
      summary.push({ dataset: dataset.name, status: `OK (${operation})`, queryId });
    } catch (err) {
      console.error(`❌ ERROR on ${dataset.name}: ${err.message}`);
      hasErrors = true;
      summary.push({ dataset: dataset.name, status: `ERROR: ${err.message}` });
    }
  }

  console.log(`\n\n=== SUMMARY ===`);
  for (const s of summary) {
    console.log(`${s.dataset.padEnd(15)} → ${s.status}`);
  }

  // Return a non-zero exit code if at least one dataset failed.
  // This is important for automation: the process must not report
  // success when one or more datasets encountered an error.
  if (hasErrors) {
    process.exitCode = 1;
  }
}

main().catch((err) => {
  console.error("Fatal error:", err);
  process.exit(1);
});

