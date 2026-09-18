const fs = require('fs');
const path = require('path');
const logger = require('./logger');

function loadConfig() {
    const configPath = path.join(__dirname, '..', 'config', 'mappings.json');

    if (!fs.existsSync(configPath)) {
        throw new Error(`Configuration file not found: ${configPath}`);
    }

    const config = JSON.parse(fs.readFileSync(configPath, 'utf8'));

    validateConfig(config);

    return selectConfiguredTable(config, process.env.GOLD_LOADER_TABLE);
}

function selectConfiguredTable(config, requestedTable) {
    const tableName = requestedTable?.trim();
    if (!tableName) return config;

    const selectedTables = config.tables.filter(
        (table) => table.sourceTable === tableName || table.targetTable === tableName
    );

    if (selectedTables.length === 0) {
        throw new Error(
            `Configuration: GOLD_LOADER_TABLE "${tableName}" does not match any sourceTable or targetTable.`
        );
    }

    logger.info(`[CONFIG] Single-table mode enabled: ${selectedTables[0].sourceTable}`);
    return { ...config, tables: selectedTables };
}

/**
 * Pre-flight validation, run BEFORE any Athena/Postgres call.
 * Catches configuration mistakes early and cheaply (no network
 * calls), instead of failing halfway through an expensive query.
 */
function validateConfig(config) {
    if (!config.settings) {
        throw new Error('Configuration: "settings" section missing.');
    }

    if (!Number.isInteger(config.settings.batchSize) || config.settings.batchSize <= 0) {
        throw new Error('Configuration: "settings.batchSize" must be a positive integer.');
    }

    if (!Array.isArray(config.tables) || config.tables.length === 0) {
        throw new Error('Configuration: "tables" must be a non-empty array.');
    }

    const seenTargetTables = new Set();

    for (const table of config.tables) {
        validateTable(table);

        // Two source tables must never write to the same Postgres
        // table — this would silently corrupt data via ON CONFLICT
        // on unrelated rows.
        if (seenTargetTables.has(table.targetTable)) {
            throw new Error(
                `Configuration: targetTable "${table.targetTable}" is used by more than one source table.`
            );
        }
        seenTargetTables.add(table.targetTable);
    }
}

function validateTable(table) {
    if (!table.sourceTable) {
        throw new Error('A table entry is missing "sourceTable".');
    }

    if (!table.targetTable) {
        throw new Error(`Table ${table.sourceTable}: "targetTable" missing.`);
    }

    if (!Array.isArray(table.primaryKeys) || table.primaryKeys.length === 0) {
        throw new Error(`Table ${table.sourceTable}: Primary Key is mandatory.`);
    }

    if (!Array.isArray(table.columns) || table.columns.length === 0) {
        throw new Error(`Table ${table.sourceTable}: no columns configured.`);
    }

    const sourceColumns = table.columns.map((c) => c.source);
    const targetColumns = table.columns.map((c) => c.target);

    // Duplicate source or target column names would silently break
    // the row mapping (later columns overwrite earlier ones) or the
    // upsert SQL (duplicate column in INSERT). Fail loudly instead.
    const dupSource = findDuplicates(sourceColumns);
    if (dupSource.length > 0) {
        throw new Error(
            `Table ${table.sourceTable}: duplicate "source" column(s): ${dupSource.join(', ')}`
        );
    }

    const dupTarget = findDuplicates(targetColumns);
    if (dupTarget.length > 0) {
        throw new Error(
            `Table ${table.sourceTable}: duplicate "target" column(s): ${dupTarget.join(', ')}`
        );
    }

    const dupPrimaryKeys = findDuplicates(table.primaryKeys);
    if (dupPrimaryKeys.length > 0) {
        throw new Error(
            `Table ${table.sourceTable}: duplicate Primary Key column(s): ${dupPrimaryKeys.join(', ')}`
        );
    }

    for (const column of table.columns) {
        if (!column.source || !column.target || !column.postgresType) {
            throw new Error(
                `Table ${table.sourceTable}: every column needs "source", "target" and "postgresType" ` +
                    `(offending entry: ${JSON.stringify(column)})`
            );
        }
    }

    for (const pk of table.primaryKeys) {
        if (!targetColumns.includes(pk)) {
            throw new Error(
                `Table ${table.sourceTable}: Primary Key "${pk}" is not present in the target columns.`
            );
        }
    }

    if (table.watermark?.enabled) {
        if (!table.watermark.sourceColumn) {
            throw new Error(`Table ${table.sourceTable}: watermark.sourceColumn missing.`);
        }

        if (!sourceColumns.includes(table.watermark.sourceColumn)) {
            throw new Error(
                `Table ${table.sourceTable}: watermark source column ` +
                    `"${table.watermark.sourceColumn}" is not present in the column mapping.`
            );
        }

        // KNOWN ISSUE, intentionally NOT auto-corrected (see project
        // discussion): if the watermark column is also part of the
        // Primary Key, a legitimate update of that column's value
        // (e.g. ordcli_r.rdatam) changes the PK itself. This means
        // an "update" is not recognized as such by ON CONFLICT — it
        // is inserted as a brand-new row instead of replacing the
        // old one, leaving the previous PK version behind. This is
        // flagged loudly here so it cannot be missed, but the
        // mapping is left as configured until the real logical key
        // of ordcli_r is verified with the source system owner.
        const watermarkTargetColumn = table.columns.find(
            (c) => c.source === table.watermark.sourceColumn
        )?.target;

        if (watermarkTargetColumn && table.primaryKeys.includes(watermarkTargetColumn)) {
            logger.warn(
                `[CONFIG] Table "${table.sourceTable}": the watermark column ` +
                    `("${table.watermark.sourceColumn}" -> "${watermarkTargetColumn}") is ALSO part of ` +
                    `the Primary Key (${table.primaryKeys.join(', ')}). If this column's value changes ` +
                    `for an existing logical record, the row will be INSERTED as a new PK combination ` +
                    `instead of being updated in place, silently leaving stale duplicate rows in the ` +
                    `Gold table. This is a known open question about the real logical key of ` +
                    `"${table.sourceTable}" and has NOT been auto-corrected. Do not treat this table's ` +
                    `data as safely deduplicated until this is verified.`
            );
        }
    }
}

function findDuplicates(values) {
    const seen = new Set();
    const dups = new Set();
    for (const v of values) {
        if (seen.has(v)) dups.add(v);
        seen.add(v);
    }
    return [...dups];
}

module.exports = { loadConfig, validateConfig, selectConfiguredTable };
