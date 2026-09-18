const test = require('node:test');
const assert = require('node:assert/strict');
const { rowToObject, isRetryable, formatBytes, getQueryTimeoutMs } = require('../src/athena');

test('rowToObject zips column names with raw values', () => {
    const row = rowToObject(['id', 'name'], ['A1', 'Foo']);
    assert.deepEqual(row, { id: 'A1', name: 'Foo' });
});

test('rowToObject turns missing/undefined values into null, not undefined', () => {
    const row = rowToObject(['id', 'name', 'extra'], ['A1', null]);
    assert.equal(row.extra, null);
});

test('rowToObject preserves empty string and the literal "0" as strings, not null', () => {
    const row = rowToObject(['a', 'b'], ['', '0']);
    assert.equal(row.a, '');
    assert.equal(row.b, '0');
});

test('isRetryable classifies throttling as retryable', () => {
    assert.equal(isRetryable({ name: 'ThrottlingException' }), true);
});

test('isRetryable classifies a network reset as retryable', () => {
    assert.equal(isRetryable({ code: 'ECONNRESET' }), true);
});

test('isRetryable classifies a generic/bad-query error as NOT retryable', () => {
    assert.equal(isRetryable({ name: 'InvalidRequestException' }), false);
});

test('formatBytes renders human-readable sizes', () => {
    assert.equal(formatBytes(500), '500 B');
    assert.equal(formatBytes(1536), '1.50 KB');
    assert.equal(formatBytes(1024 * 1024 * 3), '3.00 MB');
});

test('getQueryTimeoutMs reads a positive timeout in seconds', () => {
    assert.equal(getQueryTimeoutMs('15'), 15000);
});

test('getQueryTimeoutMs rejects invalid values', () => {
    assert.throws(() => getQueryTimeoutMs('0'), /positive integer/);
    assert.throws(() => getQueryTimeoutMs('not-a-number'), /positive integer/);
});
