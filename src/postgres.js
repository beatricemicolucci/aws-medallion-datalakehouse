const logger = require('./logger');
const { from: copyFrom } = require('pg-copy-streams');

function quoteIdentifier(identifier) {
    return '"' + identifier.replace(/"/g, '""') + '"';
}

const POSTGRES_PARAM_LIMIT = 65535;

/**
 * Computes a batch size that never exceeds Postgres' hard limit of
 * 65535 bound parameters per query ($1..$N). Pure function, easy to
 * unit test.
 *
 * Uses the SMALLER of the configured batchSize and the maximum size
 * that fits under the parameter limit — never silently exceeds the
 * limit, never silently ignores the user's configured preference
 * when it is already safe.
 */
function computeSafeBatchSize(requestedBatchSize, columnCount) {
    if (columnCount <= 0) {
        throw new Error('computeSafeBatchSize: columnCount must be > 0');
    }
    const maxRowsUnderLimit = Math.floor(POSTGRES_PARAM_LIMIT / columnCount);
    if (maxRowsUnderLimit < 1) {
        throw new Error(
            `A single row already needs ${columnCount} parameters, which exceeds the ` +
                `PostgreSQL limit of ${POSTGRES_PARAM_LIMIT}. This table cannot be batch-inserted ` +
                `with this driver.`
        );
    }
    return Math.min(requestedBatchSize, maxRowsUnderLimit);
}

function buildCreateTableSQL(config, schema) {
    const columns = config.columns.map(
        (column) => `${quoteIdentifier(column.target)} ${column.postgresType}`
    );

    const primaryKeys = config.primaryKeys.map(quoteIdentifier).join(', ');
    columns.push(`PRIMARY KEY (${primaryKeys})`);

    return `
        CREATE TABLE IF NOT EXISTS
        ${quoteIdentifier(schema)}.${quoteIdentifier(config.targetTable)}
        (
            ${columns.join(',\n            ')}
        )
    `;
}

/**
 * Reads the CURRENT state of a table from Postgres' own catalog:
 * column names/types, and which columns form the Primary Key.
 * Used to diff against the configured mapping (schema evolution).
 */
async function getExistingTableSchema(db, schema, tableName) {
    const columnsResult = await db.query(
        `
        SELECT column_name, data_type
        FROM information_schema.columns
        WHERE table_schema = $1 AND table_name = $2
        `,
        [schema, tableName]
    );

    const pkResult = await db.query(
        `
        SELECT kcu.column_name
        FROM information_schema.table_constraints tc
        JOIN information_schema.key_column_usage kcu
          ON tc.constraint_name = kcu.constraint_name
         AND tc.table_schema = kcu.table_schema
        WHERE tc.table_schema = $1
          AND tc.table_name = $2
          AND tc.constraint_type = 'PRIMARY KEY'
        `,
        [schema, tableName]
    );

    return {
        columns: columnsResult.rows.map((r) => ({ name: r.column_name, dataType: r.data_type })),
        primaryKeys: pkResult.rows.map((r) => r.column_name)
    };
}

/**
 * Pure diff function: compares the configured mapping against the
 * table's current real schema. Returns what needs to be ADDED
 * (safe, automatic), what is only present in Postgres and not in
 * the config (WARN only, never dropped automatically), and any
 * type/PK mismatch that must block execution rather than be
 * silently "fixed" (a type or PK change is a destructive,
 * data-affecting decision that a human must make explicitly).
 *
 * Extracted as a standalone function so the evolution logic is
 * testable without a real database connection.
 */
function diffSchema(config, existingSchema) {
    const existingByName = new Map(existingSchema.columns.map((c) => [c.name, c.dataType]));

    const toAdd = config.columns.filter((c) => !existingByName.has(c.target));

    const extraInDb = existingSchema.columns
        .map((c) => c.name)
        .filter((name) => !config.columns.some((c) => c.target === name));

    const typeMismatches = config.columns
        .filter((c) => existingByName.has(c.target))
        .filter((c) => !isCompatibleType(existingByName.get(c.target), c.postgresType))
        .map((c) => ({
            column: c.target,
            existingType: existingByName.get(c.target),
            configuredType: c.postgresType
        }));

    const configuredPkSet = new Set(config.primaryKeys);
    const existingPkSet = new Set(existingSchema.primaryKeys);
    const pkMismatch =
        configuredPkSet.size !== existingPkSet.size ||
        [...configuredPkSet].some((pk) => !existingPkSet.has(pk));

    return { toAdd, extraInDb, typeMismatches, pkMismatch };
}

/**
 * Very small compatibility check: Postgres reports data_type in its
 * own canonical form (e.g. "double precision", "text", "timestamp
 * without time zone"), which does not always match the literal
 * string used at CREATE TABLE time (e.g. "TIMESTAMP"). This
 * normalizes both sides for a loose but safe comparison — it is
 * intentionally conservative: when unsure, it reports a mismatch
 * rather than silently assuming compatibility.
 */
function isCompatibleType(existingType, configuredType) {
    const normalize = (t) =>
        t
            .toLowerCase()
            .replace('double precision', 'double precision')
            .replace(/^timestamp.*$/, 'timestamp')
            .replace(/^character varying.*$/, 'text')
            .replace('varchar', 'text')
            .trim();

    return normalize(existingType) === normalize(configuredType);
}

/**
 * Ensures the target table exists and matches the configured
 * mapping. New table: created from scratch. Existing table: new
 * columns are added automatically (metadata-only, non-destructive);
 * columns present in the DB but absent from the config are only
 * WARNED about; any type or Primary Key mismatch ABORTS this
 * table's processing rather than silently altering something that
 * could lose or corrupt data.
 */
async function ensureTableExists(db, config, schema) {
    const exists = await tableExists(db, schema, config.targetTable);

    if (!exists) {
        await db.query(buildCreateTableSQL(config, schema));
        logger.info(`[POSTGRES] Table created: ${schema}.${config.targetTable}`);
        return;
    }

    const existingSchema = await getExistingTableSchema(db, schema, config.targetTable);
    const diff = diffSchema(config, existingSchema);

    if (diff.pkMismatch) {
        throw new Error(
            `Table ${schema}.${config.targetTable}: configured Primary Key ` +
                `(${config.primaryKeys.join(', ')}) does not match the existing table's Primary Key ` +
                `(${existingSchema.primaryKeys.join(', ')}). Refusing to proceed automatically — ` +
                `changing a Primary Key can silently duplicate or lose data. Resolve this manually.`
        );
    }

    if (diff.typeMismatches.length > 0) {
        const details = diff.typeMismatches
            .map((m) => `${m.column} (existing: ${m.existingType}, configured: ${m.configuredType})`)
            .join('; ');
        throw new Error(
            `Table ${schema}.${config.targetTable}: type mismatch on column(s): ${details}. ` +
                `Refusing to ALTER COLUMN TYPE automatically — this can be a lossy, irreversible ` +
                `operation. Resolve this manually.`
        );
    }

    if (diff.extraInDb.length > 0) {
        logger.warn(
            `[POSTGRES] Table ${schema}.${config.targetTable}: column(s) present in the database ` +
                `but not in the current mapping: ${diff.extraInDb.join(', ')}. They are left untouched ` +
                `(never dropped automatically).`
        );
    }

    if (diff.toAdd.length > 0) {
        for (const column of diff.toAdd) {
            const sql = `
                ALTER TABLE ${quoteIdentifier(schema)}.${quoteIdentifier(config.targetTable)}
                ADD COLUMN ${quoteIdentifier(column.target)} ${column.postgresType}
            `;
            await db.query(sql);
            logger.info(
                `[POSTGRES] Table ${schema}.${config.targetTable}: added column "${column.target}" (${column.postgresType})`
            );
        }
    }

    logger.info(`[POSTGRES] Table verified: ${schema}.${config.targetTable}`);
}

async function tableExists(db, schema, tableName) {
    const result = await db.query(
        `SELECT 1 FROM information_schema.tables WHERE table_schema = $1 AND table_name = $2`,
        [schema, tableName]
    );
    return result.rows.length > 0;
}

function buildUpsertSQL(config, schema, rows) {
    const columns = config.columns.map((column) => column.target);
    const columnList = columns.map(quoteIdentifier).join(', ');

    const values = [];
    const placeholders = [];
    let parameterIndex = 1;

    for (const row of rows) {
        const rowPlaceholders = [];
        for (const column of config.columns) {
            // Nullish coalescing (??) only replaces null/undefined:
            // 0, false and '' are legitimate values and must be
            // preserved as-is, never silently turned into NULL.
            values.push(row[column.target] ?? null);
            rowPlaceholders.push(`$${parameterIndex++}`);
        }
        placeholders.push(`(${rowPlaceholders.join(', ')})`);
    }

    const pkSet = new Set(config.primaryKeys);
    const updateColumns = columns
        .filter((column) => !pkSet.has(column))
        .map((column) => `${quoteIdentifier(column)} = EXCLUDED.${quoteIdentifier(column)}`);

    // If every column is part of the Primary Key, there is nothing
    // left to UPDATE on conflict — fall back to DO NOTHING instead
    // of generating invalid SQL with an empty SET clause.
    const conflictAction =
        updateColumns.length > 0
            ? `DO UPDATE SET\n            ${updateColumns.join(',\n            ')}`
            : 'DO NOTHING';

    const sql = `
        INSERT INTO
        ${quoteIdentifier(schema)}.${quoteIdentifier(config.targetTable)}
        (${columnList})
        VALUES
        ${placeholders.join(',\n')}
        ON CONFLICT (${config.primaryKeys.map(quoteIdentifier).join(', ')})
        ${conflictAction}
    `;

    return { sql, values };
}

async function upsertBatch(db, config, schema, rows) {
    if (rows.length === 0) return;
    const { sql, values } = buildUpsertSQL(config, schema, rows);
    // A single INSERT statement is atomic in Postgres by default:
    // this batch either fully commits or fully fails, so a partial
    // batch can never be left half-written.
    await db.query(sql, values);
}

function buildCopySQL(config, schema, stagingTable) {
    const columns = config.columns.map((column) => quoteIdentifier(column.target)).join(', ');
    return `COPY ${quoteIdentifier(stagingTable)} (${columns}) FROM STDIN WITH (FORMAT csv, NULL '\\N')`;
}

function buildMergeSQL(config, schema, stagingTable) {
    const columns = config.columns.map((column) => column.target);
    const columnList = columns.map(quoteIdentifier).join(', ');
    const pkSet = new Set(config.primaryKeys);
    const updateColumns = columns
        .filter((column) => !pkSet.has(column))
        .map((column) => `${quoteIdentifier(column)} = EXCLUDED.${quoteIdentifier(column)}`);
    const conflictAction =
        updateColumns.length > 0
            ? `DO UPDATE SET\n            ${updateColumns.join(',\n            ')}`
            : 'DO NOTHING';

    return `
        INSERT INTO ${quoteIdentifier(schema)}.${quoteIdentifier(config.targetTable)} (${columnList})
        SELECT ${columnList}
        FROM ${quoteIdentifier(stagingTable)}
        ON CONFLICT (${config.primaryKeys.map(quoteIdentifier).join(', ')})
        ${conflictAction}
    `;
}

function csvValue(value) {
    if (value === null || value === undefined) return '\\N';
    return `"${String(value).replace(/"/g, '""')}"`;
}

async function copyToStaging(client, config, schema, stagingTable, rows) {
    const columns = config.columns.map((column) => quoteIdentifier(column.target)).join(', ');
    await client.query(
        `CREATE TEMP TABLE ${quoteIdentifier(stagingTable)} AS
         SELECT ${columns} FROM ${quoteIdentifier(schema)}.${quoteIdentifier(config.targetTable)} WITH NO DATA`
    );

    const copyStream = client.query(copyFrom(buildCopySQL(config, schema, stagingTable)));
    let totalRows = 0;

    try {
        for await (const row of rows) {
            const line = config.columns.map((column) => csvValue(row[column.target])).join(',') + '\n';
            if (!copyStream.write(line)) {
                await new Promise((resolve, reject) => {
                    copyStream.once('drain', resolve);
                    copyStream.once('error', reject);
                });
            }
            totalRows++;
        }
        copyStream.end();
        await new Promise((resolve, reject) => {
            copyStream.once('finish', resolve);
            copyStream.once('error', reject);
        });
    } catch (err) {
        copyStream.destroy(err);
        throw err;
    }

    return totalRows;
}

async function copyAndMerge(db, config, schema, rows) {
    const client = await db.connect();
    const stagingTable = `gold_loader_staging_${process.pid}_${Date.now()}`;

    try {
        await client.query('BEGIN');
        const totalRows = await copyToStaging(client, config, schema, stagingTable, rows);
        if (totalRows > 0) {
            await client.query(buildMergeSQL(config, schema, stagingTable));
        }
        await client.query('COMMIT');
        return totalRows;
    } catch (err) {
        await client.query('ROLLBACK').catch(() => {});
        throw err;
    } finally {
        client.release();
    }
}

module.exports = {
    ensureTableExists,
    upsertBatch,
    copyAndMerge,
    computeSafeBatchSize,
    buildUpsertSQL,
    buildCopySQL,
    buildMergeSQL,
    buildCreateTableSQL,
    diffSchema,
    isCompatibleType,
    quoteIdentifier
};
