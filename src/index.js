require('dotenv').config();

const { Pool } = require('pg');

const { loadConfig } = require('./config');
const { executeQuery } = require('./athena');
const { ensureTableExists, upsertBatch, computeSafeBatchSize } = require('./postgres');
const { ensureWatermarkTable, getWatermark, saveWatermark } = require('./watermark');
const { acquireRunLock, releaseRunLock } = require('./lock');
const logger = require('./logger');

const config = loadConfig();

const db = new Pool({
    connectionString: process.env.DATABASE_URL,
    max: 5,
    idleTimeoutMillis: 30000,
    connectionTimeoutMillis: 10000
});

function escapeAthenaIdentifier(identifier) {
    return `"${identifier.replace(/"/g, '""')}"`;
}

function formatAthenaTimestamp(date) {
    const iso = new Date(date).toISOString();
    return iso.replace('T', ' ').replace('Z', '');
}

/**
 * Builds the Athena SELECT. When a watermark exists, the query is
 * filtered to only the rows modified since then (minus a small
 * overlap window, to tolerate clock skew / late-arriving writes) —
 * this is what avoids scanning the entire Silver table on every run.
 * Without a watermark (first run, or watermark disabled), this is
 * necessarily a full scan: unavoidable for an initial load, but
 * worth knowing when estimating Athena cost for large tables.
 */
function buildSelect(tableConfig, watermark) {
    const sourceColumns = tableConfig.columns.map((column) => escapeAthenaIdentifier(column.source)).join(', ');

    let sql = `
        SELECT
            ${sourceColumns}
        FROM ${escapeAthenaIdentifier(tableConfig.sourceTable)}
    `;

    if (tableConfig.watermark?.enabled && watermark) {
        const overlapSeconds = Number(config.settings.watermarkOverlapSeconds || 60);
        const lowerBound = new Date(new Date(watermark).getTime() - overlapSeconds * 1000);

        sql += `
            WHERE ${escapeAthenaIdentifier(tableConfig.watermark.sourceColumn)}
            >= TIMESTAMP '${formatAthenaTimestamp(lowerBound)}'
        `;
    }

    return sql;
}

function mapRow(tableConfig, sourceRow) {
    const targetRow = {};
    for (const column of tableConfig.columns) {
        // See postgres.js buildUpsertSQL for the same rule: ?? only
        // replaces null/undefined, never 0, false or ''.
        targetRow[column.target] = sourceRow[column.source] ?? null;
    }
    return targetRow;
}

function getRowWatermark(tableConfig, sourceRow) {
    if (!tableConfig.watermark?.enabled) return null;

    const value = sourceRow[tableConfig.watermark.sourceColumn];
    if (value === null || value === undefined) return null;

    const date = new Date(value);
    if (Number.isNaN(date.getTime())) return null;

    return date;
}

async function processTable(tableConfig, settings) {
    const startTime = Date.now();

    logger.info('==================================================');
    logger.info(`Start: ${tableConfig.sourceTable} -> ${tableConfig.targetTable}`);

    await ensureTableExists(db, tableConfig, settings.postgresSchema);

    // Batch size is capped here, once per table, based on that
    // table's own column count — a wide table (e.g. ANACLIENTI,
    // ~80 columns) gets a smaller safe batch than a narrow one, but
    // the configured batchSize is still respected whenever it is
    // already safe.
    const safeBatchSize = computeSafeBatchSize(settings.batchSize, tableConfig.columns.length);
    if (safeBatchSize < settings.batchSize) {
        logger.warn(
            `[${tableConfig.targetTable}] Configured batchSize (${settings.batchSize}) would exceed ` +
                `PostgreSQL's parameter limit for ${tableConfig.columns.length} columns. ` +
                `Using ${safeBatchSize} instead.`
        );
    }

    const previousWatermark = tableConfig.watermark?.enabled
        ? await getWatermark(db, tableConfig.sourceTable)
        : null;

    if (previousWatermark) {
        logger.info(`Previous watermark: ${previousWatermark.toISOString()}`);
    } else if (tableConfig.watermark?.enabled) {
        logger.info('No watermark present: performing a full initial load.');
    }

    const sql = buildSelect(tableConfig, previousWatermark);
    const sourceColumns = tableConfig.columns.map((column) => column.source);

    const batch = [];
    let totalRows = 0;
    let batchNumber = 0;
    let maxWatermark = previousWatermark ? new Date(previousWatermark) : null;

    try {
        for await (const sourceRow of executeQuery(sql, sourceColumns)) {
            const targetRow = mapRow(tableConfig, sourceRow);
            batch.push(targetRow);

            const rowWatermark = getRowWatermark(tableConfig, sourceRow);
            if (rowWatermark && (!maxWatermark || rowWatermark > maxWatermark)) {
                maxWatermark = rowWatermark;
            }

            if (batch.length >= safeBatchSize) {
                batchNumber++;
                await upsertBatch(db, tableConfig, settings.postgresSchema, batch);
                totalRows += batch.length;
                logger.info(`[${tableConfig.targetTable}] Batch ${batchNumber} completed: ${batch.length} rows`);
                batch.length = 0;
            }
        }

        if (batch.length > 0) {
            batchNumber++;
            await upsertBatch(db, tableConfig, settings.postgresSchema, batch);
            totalRows += batch.length;
            logger.info(`[${tableConfig.targetTable}] Batch ${batchNumber} completed: ${batch.length} rows`);
        }

        // The watermark is advanced ONLY after every batch of this
        // table has committed successfully. If any batch above threw,
        // execution never reaches this point, and the watermark stays
        // at its previous value — the next run will safely re-read
        // (and idempotently re-upsert) the same window of rows.
        if (tableConfig.watermark?.enabled && maxWatermark) {
            await saveWatermark(db, tableConfig.sourceTable, maxWatermark);
            logger.info(`[${tableConfig.targetTable}] New watermark: ${maxWatermark.toISOString()}`);
        }

        const elapsedSeconds = ((Date.now() - startTime) / 1000).toFixed(2);
        logger.success(`${tableConfig.sourceTable} completed. Rows processed: ${totalRows}. Time: ${elapsedSeconds}s`);
    } catch (err) {
        logger.error(`Error while loading ${tableConfig.sourceTable}`, err);
        throw err;
    }
}

/**
 * DELETIONS — deliberately NOT implemented.
 *
 * The Silver layer (see project history) does not currently carry
 * an explicit "deleted" signal from the source ERP: a record that
 * stops appearing in new Bronze extracts simply stops being updated
 * in Silver, it is not flagged as removed. Combined with this
 * loader's incremental, watermark-based reads (which only ever look
 * at ROWS THAT CHANGED, never at "which PKs disappeared"), Gold has
 * no reliable signal to distinguish "this record was deleted at the
 * source" from "this record simply had no recent changes".
 *
 * Implementing a destructive DELETE here based on absence would risk
 * removing perfectly valid, simply-unchanged rows. The safe options,
 * NOT implemented here to avoid over-engineering a need that is not
 * yet confirmed, would be:
 *   - a periodic FULL reconciliation pass (unfiltered read of all
 *     source PKs, compared against all Gold PKs, with a soft
 *     "is_deleted"/"last_seen_at" flag rather than a hard DELETE), or
 *   - an explicit deletion feed from the source system, if one ever
 *     becomes available upstream.
 * Until one of these is confirmed as feasible/needed, Gold tables
 * are append/update-only and may contain rows no longer present at
 * the source.
 */

async function main() {
    let lockClient;

    try {
        logger.info('Gold Loader starting');
        logger.info(`Tables configured: ${config.tables.length}`);

        lockClient = await acquireRunLock(db);
        if (!lockClient) {
            logger.warn('Another Gold Loader instance appears to be running. Exiting without doing any work.');
            process.exitCode = 1;
            return;
        }

        await ensureWatermarkTable(db);

        let failedTables = 0;

        for (const tableConfig of config.tables) {
            try {
                await processTable(tableConfig, config.settings);
            } catch (err) {
                failedTables++;
                logger.error(`Table ${tableConfig.sourceTable} FAILED. Other tables will still be processed.`, err);
            }
        }

        if (failedTables > 0) {
            logger.warn(`Gold Loader finished with ${failedTables} failed table(s).`);
            process.exitCode = 1;
        } else {
            logger.success('Gold Loader finished successfully.');
        }
    } catch (err) {
        logger.error('Fatal Gold Loader error', err);
        process.exitCode = 1;
    } finally {
        await releaseRunLock(lockClient);
        await db.end();
        logger.info('PostgreSQL connection closed.');
    }
}

main();
