const {
    AthenaClient,
    StartQueryExecutionCommand,
    GetQueryExecutionCommand,
    GetQueryResultsCommand
} = require('@aws-sdk/client-athena');

const logger = require('./logger');

const athena = new AthenaClient({
    region: process.env.AWS_REGION || 'eu-central-1'
});

const DEFAULT_QUERY_TIMEOUT_SECONDS = 3600;

function sleep(ms) {
    return new Promise((resolve) => setTimeout(resolve, ms));
}

// Transient AWS errors worth retrying. Anything else (bad SQL,
// missing table, permission denied) should fail immediately instead
// of wasting time retrying something that will never succeed.
const RETRYABLE_ERROR_NAMES = new Set([
    'ThrottlingException',
    'TooManyRequestsException',
    'InternalServerException',
    'RequestTimeout'
]);

function isRetryable(err) {
    if (RETRYABLE_ERROR_NAMES.has(err.name)) return true;
    // Network-level errors from the AWS SDK/Node http stack
    if (['ECONNRESET', 'ETIMEDOUT', 'EAI_AGAIN'].includes(err.code)) return true;
    return false;
}

/**
 * Runs an async AWS SDK call with exponential backoff retry, only
 * for errors classified as transient. Kept intentionally simple
 * (fixed max attempts, no jitter library) to avoid over-engineering
 * a concern that, at this project's query volume, rarely triggers.
 */
async function withRetry(fn, { maxAttempts = 4, baseDelayMs = 1000 } = {}) {
    let lastErr;
    for (let attempt = 1; attempt <= maxAttempts; attempt++) {
        try {
            return await fn();
        } catch (err) {
            lastErr = err;
            if (!isRetryable(err) || attempt === maxAttempts) {
                throw err;
            }
            const delay = baseDelayMs * 2 ** (attempt - 1);
            logger.warn(
                `[ATHENA] Transient error (${err.name || err.code}), retry ${attempt}/${maxAttempts} in ${delay}ms`
            );
            await sleep(delay);
        }
    }
    throw lastErr;
}

function getQueryTimeoutMs(rawValue = process.env.ATHENA_QUERY_TIMEOUT_SECONDS) {
    if (rawValue === undefined || rawValue === '') {
        return DEFAULT_QUERY_TIMEOUT_SECONDS * 1000;
    }

    const seconds = Number(rawValue);
    if (!Number.isSafeInteger(seconds) || seconds <= 0) {
        throw new Error('ATHENA_QUERY_TIMEOUT_SECONDS must be a positive integer.');
    }

    return seconds * 1000;
}

async function waitForQuery(queryExecutionId, timeoutMs = getQueryTimeoutMs()) {
    const controller = new AbortController();
    const deadline = Date.now() + timeoutMs;
    const timeout = setTimeout(() => controller.abort(), timeoutMs);

    try {
        while (true) {
            const response = await withRetry(() =>
                athena.send(
                    new GetQueryExecutionCommand({ QueryExecutionId: queryExecutionId }),
                    { abortSignal: controller.signal }
                )
            );

            const status = response.QueryExecution.Status;
            const state = status.State;

            if (state === 'SUCCEEDED') {
                const stats = response.QueryExecution.Statistics;
                const bytesScanned = stats?.DataScannedInBytes;
                const execMs = stats?.EngineExecutionTimeInMillis;

                logger.info(
                    `[ATHENA] Query ${queryExecutionId} succeeded` +
                        (bytesScanned !== undefined ? ` — ${formatBytes(bytesScanned)} scanned` : '') +
                        (execMs !== undefined ? `, ${execMs}ms engine time` : '')
                );
                return;
            }

            if (state === 'FAILED' || state === 'CANCELLED') {
                const reason = status.StateChangeReason || 'No reason available';
                throw new Error(`Athena query ${queryExecutionId} ${state}: ${reason}`);
            }

            const remainingMs = deadline - Date.now();
            if (remainingMs <= 0) {
                throw new Error(`Athena query ${queryExecutionId} timed out after ${timeoutMs}ms.`);
            }

            await sleep(Math.min(2000, remainingMs));
        }
    } catch (err) {
        if (controller.signal.aborted) {
            throw new Error(`Athena query ${queryExecutionId} timed out after ${timeoutMs}ms.`, { cause: err });
        }
        throw err;
    } finally {
        clearTimeout(timeout);
    }
}

function formatBytes(bytes) {
    if (bytes < 1024) return `${bytes} B`;
    const units = ['KB', 'MB', 'GB', 'TB'];
    let value = bytes;
    let unitIndex = -1;
    do {
        value /= 1024;
        unitIndex++;
    } while (value >= 1024 && unitIndex < units.length - 1);
    return `${value.toFixed(2)} ${units[unitIndex]}`;
}

async function startQuery(sql) {
    logger.info(`[ATHENA] Starting query: ${sql.replace(/\s+/g, ' ').trim()}`);

    const response = await withRetry(() =>
        athena.send(
            new StartQueryExecutionCommand({
                QueryString: sql,
                QueryExecutionContext: {
                    Database: process.env.ATHENA_DATABASE || 'silver_dev'
                },
                WorkGroup: process.env.ATHENA_WORKGROUP || 'primary',
                ResultConfiguration: {
                    OutputLocation: process.env.ATHENA_OUTPUT
                }
            })
        )
    );

    const queryExecutionId = response.QueryExecutionId;

    if (!queryExecutionId) {
        throw new Error('Athena did not return a QueryExecutionId.');
    }

    logger.info(`[ATHENA] QueryExecutionId: ${queryExecutionId}`);

    await waitForQuery(queryExecutionId, getQueryTimeoutMs());

    return queryExecutionId;
}

async function* streamQueryResults(queryExecutionId) {
    let nextToken;
    let isFirstPage = true;

    while (true) {
        const response = await withRetry(() =>
            athena.send(
                new GetQueryResultsCommand({
                    QueryExecutionId: queryExecutionId,
                    NextToken: nextToken,
                    MaxResults: 1000
                })
            )
        );

        const rows = response.ResultSet?.Rows || [];

        // Athena returns the header as the first row of the FIRST
        // page only — subsequent pages contain data rows only.
        let startIndex = isFirstPage && rows.length > 0 ? 1 : 0;

        for (let i = startIndex; i < rows.length; i++) {
            const values = rows[i].Data || [];
            yield values.map((cell) => cell?.VarCharValue ?? null);
        }

        nextToken = response.NextToken;
        isFirstPage = false;

        if (!nextToken) break;
    }
}

/**
 * Pure helper: zips a row of raw string values with column names
 * into a plain object. Extracted as a standalone function so it can
 * be unit-tested without mocking the AWS SDK.
 */
function rowToObject(columnNames, values) {
    const row = {};
    columnNames.forEach((column, index) => {
        row[column] = values[index] ?? null;
    });
    return row;
}

async function* executeQuery(sql, columnNames) {
    const queryExecutionId = await startQuery(sql);

    for await (const values of streamQueryResults(queryExecutionId)) {
        yield rowToObject(columnNames, values);
    }
}

module.exports = {
    executeQuery,
    rowToObject,
    withRetry,
    isRetryable,
    formatBytes,
    getQueryTimeoutMs
};
