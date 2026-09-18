const logger = require('./logger');

// Arbitrary but fixed 32-bit key identifying "the Gold Loader run"
// as a whole. A single global lock (not per-table) is intentional:
// it is the simplest way to guarantee no two loader processes ever
// touch the same watermark/table concurrently, without needing a
// separate lock key per table. Over-engineering this into a
// per-table lock is not justified at this project's scale (a single
// sequential run through 6 tables).
const LOCK_KEY = 727391;

/**
 * Acquires a PostgreSQL session-level advisory lock using a
 * DEDICATED client (not the pool), because advisory locks are tied
 * to the database session that took them — releasing/holding it
 * must happen on that exact same connection, which a pool cannot
 * guarantee across separate pool.query() calls.
 *
 * Returns null if the lock is already held by another process
 * (non-blocking check via pg_try_advisory_lock), so the caller can
 * exit cleanly instead of queuing up.
 */
async function acquireRunLock(pool) {
    const client = await pool.connect();

    const result = await client.query('SELECT pg_try_advisory_lock($1) AS acquired', [LOCK_KEY]);
    const acquired = result.rows[0].acquired;

    if (!acquired) {
        client.release();
        return null;
    }

    logger.info('[LOCK] Advisory lock acquired — no other Gold Loader instance is running.');
    return client;
}

/**
 * Releases the lock and returns the dedicated client to the pool.
 * Must be called with the SAME client object returned by
 * acquireRunLock.
 */
async function releaseRunLock(client) {
    if (!client) return;
    try {
        await client.query('SELECT pg_advisory_unlock($1)', [LOCK_KEY]);
        logger.info('[LOCK] Advisory lock released.');
    } finally {
        client.release();
    }
}

module.exports = { acquireRunLock, releaseRunLock };
