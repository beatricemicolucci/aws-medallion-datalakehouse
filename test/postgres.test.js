const test = require('node:test');
const assert = require('node:assert/strict');
const {
    computeSafeBatchSize,
    buildUpsertSQL,
    diffSchema,
    isCompatibleType
} = require('../src/postgres');

test('computeSafeBatchSize respects the configured size when already safe', () => {
    assert.equal(computeSafeBatchSize(500, 10), 500);
});

test('computeSafeBatchSize caps the batch size to stay under 65535 parameters', () => {
    // 85 columns (roughly ANACLIENTI's width) * 800 requested rows = 68000 params, unsafe
    const safe = computeSafeBatchSize(800, 85);
    assert.ok(safe * 85 <= 65535, `safe batch * columns (${safe * 85}) must stay <= 65535`);
    assert.ok(safe < 800);
});

test('computeSafeBatchSize throws if a single row already exceeds the limit', () => {
    assert.throws(() => computeSafeBatchSize(10, 70000), /exceeds the PostgreSQL limit/);
});

const sampleConfig = {
    targetTable: 'dim_test',
    primaryKeys: ['id'],
    columns: [
        { source: 's_id', target: 'id', postgresType: 'TEXT' },
        { source: 's_flag', target: 'flag', postgresType: 'BOOLEAN' },
        { source: 's_qty', target: 'quantity', postgresType: 'DOUBLE PRECISION' },
        { source: 's_note', target: 'note', postgresType: 'TEXT' }
    ]
};

test('buildUpsertSQL produces one parameter per column per row', () => {
    const rows = [
        { id: 'A1', flag: false, quantity: 0, note: '' },
        { id: 'A2', flag: true, quantity: 5, note: 'hello' }
    ];
    const { sql, values } = buildUpsertSQL(sampleConfig, 'public', rows);

    assert.equal(values.length, rows.length * sampleConfig.columns.length);
    assert.match(sql, /ON CONFLICT \("id"\)/);
    assert.match(sql, /DO UPDATE SET/);
});

test('buildUpsertSQL preserves 0, false and empty string — never turns them into NULL', () => {
    const rows = [{ id: 'A1', flag: false, quantity: 0, note: '' }];
    const { values } = buildUpsertSQL(sampleConfig, 'public', rows);

    // order follows sampleConfig.columns: id, flag, quantity, note
    assert.equal(values[0], 'A1');
    assert.equal(values[1], false);
    assert.equal(values[2], 0);
    assert.equal(values[3], '');
});

test('buildUpsertSQL turns null/undefined into NULL, distinct from falsy values', () => {
    const rows = [{ id: 'A1', flag: null, quantity: undefined, note: 'x' }];
    const { values } = buildUpsertSQL(sampleConfig, 'public', rows);

    assert.equal(values[1], null);
    assert.equal(values[2], null);
});

test('buildUpsertSQL falls back to DO NOTHING when every column is part of the PK', () => {
    const allPkConfig = {
        targetTable: 'link_table',
        primaryKeys: ['a', 'b'],
        columns: [
            { source: 'a', target: 'a', postgresType: 'TEXT' },
            { source: 'b', target: 'b', postgresType: 'TEXT' }
        ]
    };
    const { sql } = buildUpsertSQL(allPkConfig, 'public', [{ a: '1', b: '2' }]);
    assert.match(sql, /DO NOTHING/);
    assert.doesNotMatch(sql, /DO UPDATE SET/);
});

test('diffSchema detects columns to add, without flagging false mismatches', () => {
    const existingSchema = {
        columns: [
            { name: 'id', dataType: 'text' },
            { name: 'flag', dataType: 'boolean' }
        ],
        primaryKeys: ['id']
    };
    const diff = diffSchema(sampleConfig, existingSchema);

    assert.deepEqual(
        diff.toAdd.map((c) => c.target),
        ['quantity', 'note']
    );
    assert.deepEqual(diff.extraInDb, []);
    assert.equal(diff.pkMismatch, false);
});

test('diffSchema warns (extraInDb) about columns removed from the mapping, without suggesting a drop', () => {
    const existingSchema = {
        columns: [
            { name: 'id', dataType: 'text' },
            { name: 'flag', dataType: 'boolean' },
            { name: 'quantity', dataType: 'double precision' },
            { name: 'note', dataType: 'text' },
            { name: 'legacy_column', dataType: 'text' }
        ],
        primaryKeys: ['id']
    };
    const diff = diffSchema(sampleConfig, existingSchema);
    assert.deepEqual(diff.extraInDb, ['legacy_column']);
});

test('diffSchema flags a type mismatch instead of silently ignoring it', () => {
    const existingSchema = {
        columns: [
            { name: 'id', dataType: 'text' },
            { name: 'flag', dataType: 'text' } // was BOOLEAN in config
        ],
        primaryKeys: ['id']
    };
    const diff = diffSchema(sampleConfig, existingSchema);
    assert.equal(diff.typeMismatches.length, 1);
    assert.equal(diff.typeMismatches[0].column, 'flag');
});

test('diffSchema flags a Primary Key mismatch', () => {
    const existingSchema = {
        columns: sampleConfig.columns.map((c) => ({ name: c.target, dataType: 'text' })),
        primaryKeys: ['note'] // different PK than configured ('id')
    };
    const diff = diffSchema(sampleConfig, existingSchema);
    assert.equal(diff.pkMismatch, true);
});

test('isCompatibleType normalizes common Postgres type spellings', () => {
    assert.equal(isCompatibleType('double precision', 'DOUBLE PRECISION'), true);
    assert.equal(isCompatibleType('timestamp without time zone', 'TIMESTAMP'), true);
    assert.equal(isCompatibleType('text', 'TEXT'), true);
    assert.equal(isCompatibleType('boolean', 'TEXT'), false);
});
