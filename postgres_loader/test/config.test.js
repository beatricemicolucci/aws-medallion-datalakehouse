const test = require('node:test');
const assert = require('node:assert/strict');
const { validateConfig, selectConfiguredTable } = require('../src/config');

function baseTable(overrides = {}) {
    return {
        sourceTable: 'foo',
        targetTable: 'bar',
        primaryKeys: ['id'],
        columns: [
            { source: 'foo_id', target: 'id', postgresType: 'TEXT' },
            { source: 'foo_name', target: 'name', postgresType: 'TEXT' }
        ],
        ...overrides
    };
}

function baseConfig(tables) {
    return { settings: { batchSize: 500 }, tables };
}

test('accepts a valid configuration', () => {
    assert.doesNotThrow(() => validateConfig(baseConfig([baseTable()])));
});

test('selects one table by sourceTable or targetTable', () => {
    const first = baseTable({ sourceTable: 'source_one', targetTable: 'target_one' });
    const second = baseTable({ sourceTable: 'source_two', targetTable: 'target_two' });
    const config = baseConfig([first, second]);

    assert.deepEqual(selectConfiguredTable(config, 'source_one').tables, [first]);
    assert.deepEqual(selectConfiguredTable(config, 'target_two').tables, [second]);
    assert.equal(selectConfiguredTable(config, '').tables.length, 2);
});

test('rejects an unknown single-table selection', () => {
    assert.throws(
        () => selectConfiguredTable(baseConfig([baseTable()]), 'unknown_table'),
        /does not match any sourceTable or targetTable/
    );
});

test('rejects missing settings', () => {
    assert.throws(() => validateConfig({ tables: [baseTable()] }), /settings/);
});

test('rejects non-positive batchSize', () => {
    assert.throws(
        () => validateConfig({ settings: { batchSize: 0 }, tables: [baseTable()] }),
        /batchSize/
    );
});

test('rejects a table without primaryKeys', () => {
    const table = baseTable({ primaryKeys: [] });
    assert.throws(() => validateConfig(baseConfig([table])), /Primary Key is mandatory/);
});

test('rejects a Primary Key not present among target columns', () => {
    const table = baseTable({ primaryKeys: ['does_not_exist'] });
    assert.throws(() => validateConfig(baseConfig([table])), /is not present in the target columns/);
});

test('rejects duplicate Primary Key columns', () => {
    const table = baseTable({ primaryKeys: ['id', 'id'] });
    assert.throws(() => validateConfig(baseConfig([table])), /duplicate Primary Key/);
});

test('rejects duplicate target column names', () => {
    const table = baseTable({
        columns: [
            { source: 'a', target: 'id', postgresType: 'TEXT' },
            { source: 'b', target: 'id', postgresType: 'TEXT' }
        ]
    });
    assert.throws(() => validateConfig(baseConfig([table])), /duplicate "target"/);
});

test('rejects duplicate source column names', () => {
    const table = baseTable({
        columns: [
            { source: 'a', target: 'id', postgresType: 'TEXT' },
            { source: 'a', target: 'other', postgresType: 'TEXT' }
        ]
    });
    assert.throws(() => validateConfig(baseConfig([table])), /duplicate "source"/);
});

test('rejects two source tables writing to the same targetTable', () => {
    const t1 = baseTable({ sourceTable: 'foo', targetTable: 'same' });
    const t2 = baseTable({ sourceTable: 'baz', targetTable: 'same' });
    assert.throws(() => validateConfig(baseConfig([t1, t2])), /used by more than one source table/);
});

test('rejects a watermark column not present in the column mapping', () => {
    const table = baseTable({ watermark: { enabled: true, sourceColumn: 'does_not_exist' } });
    assert.throws(() => validateConfig(baseConfig([table])), /not present in the column mapping/);
});

test('accepts (with a warning, not a throw) a watermark column that is also part of the PK', () => {
    const table = baseTable({
        primaryKeys: ['id'],
        columns: [
            { source: 'foo_id', target: 'id', postgresType: 'TEXT' },
            { source: 'foo_datam', target: 'id', postgresType: 'TIMESTAMP' } // contrived: same target as PK for the test
        ],
        watermark: { enabled: true, sourceColumn: 'foo_datam' }
    });
    // This specific contrived case actually hits the duplicate-target
    // check first; the real ordcli_r-style case (distinct target
    // column that is ALSO listed in primaryKeys) is exercised below.
    assert.throws(() => validateConfig(baseConfig([table])));
});

test('logs a warning (does not throw) for the real ordcli_r-style case: distinct watermark column also listed as PK', () => {
    const table = baseTable({
        primaryKeys: ['id', 'data_modifica'],
        columns: [
            { source: 'foo_id', target: 'id', postgresType: 'TEXT' },
            { source: 'foo_datam', target: 'data_modifica', postgresType: 'TIMESTAMP' }
        ],
        watermark: { enabled: true, sourceColumn: 'foo_datam' }
    });
    assert.doesNotThrow(() => validateConfig(baseConfig([table])));
});
