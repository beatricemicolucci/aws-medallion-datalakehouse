async function ensureWatermarkTable(db) {
    await db.query(`
        CREATE TABLE IF NOT EXISTS gold_loader_watermarks (
            source_table TEXT PRIMARY KEY,
            last_watermark TIMESTAMP NULL,
            updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
        )
    `);
}

async function getWatermark(db, sourceTable) {
    const result = await db.query(
        `SELECT last_watermark FROM gold_loader_watermarks WHERE source_table = $1`,
        [sourceTable]
    );
    if (result.rows.length === 0) return null;
    return result.rows[0].last_watermark;
}

/**
 * Saved ONLY after a table's batches have all committed successfully
 * (see index.js): if any batch fails, this is never called, so the
 * next run naturally resumes from the previous watermark and simply
 * re-upserts already-loaded rows — harmless, since upserts are
 * idempotent by Primary Key.
 */
async function saveWatermark(db, sourceTable, watermark) {
    await db.query(
        `
        INSERT INTO gold_loader_watermarks (source_table, last_watermark)
        VALUES ($1, $2)
        ON CONFLICT (source_table)
        DO UPDATE SET
            last_watermark = EXCLUDED.last_watermark,
            updated_at = CURRENT_TIMESTAMP
        `,
        [sourceTable, watermark]
    );
}

module.exports = { ensureWatermarkTable, getWatermark, saveWatermark };
