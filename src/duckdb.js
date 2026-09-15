import duckdb from 'duckdb';

const region = process.env.AWS_REGION ?? 'eu-central-1';
const cachePath = process.env.CRAWLER_CACHE_PATH ?? './crawler-cache.duckdb';

let db = null;

export async function openCache() {
  if (db) return;

  db = new duckdb.Database(cachePath);

  await runQuery(
    db,
    `
    CREATE OR REPLACE SECRET aws_s3 (
      TYPE s3,
      PROVIDER credential_chain,
      REGION '${region}'
    );
    `,
  );

  await runQuery(
    db,
    `
    CREATE TABLE IF NOT EXISTS crawler_files (
      dataset VARCHAR NOT NULL,
      s3_key VARCHAR NOT NULL,
      etag VARCHAR,
      file_size BIGINT,
      last_modified TIMESTAMP,
      schema_json VARCHAR,
      processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
      PRIMARY KEY (dataset, s3_key)
    );
    `,
  );
}

export async function getCachedFile(dataset, s3Key) {
  ensureDatabase();

  const rows = await runQuery(
    db,
    `
    SELECT
      dataset,
      s3_key,
      etag,
      file_size,
      last_modified,
      processed_at
    FROM crawler_files
    WHERE dataset = ?
      AND s3_key = ?;
    `,
    [dataset, s3Key],
  );

  return rows[0] ?? null;
}

export async function saveCachedFile({
  dataset,
  s3Key,
  etag,
  fileSize,
  lastModified,
  schema,
}) {
  ensureDatabase();

  await runQuery(
    db,
    `
    INSERT OR REPLACE INTO crawler_files (
      dataset,
      s3_key,
      etag,
      file_size,
      last_modified,
      schema_json,
      processed_at
    )
    VALUES (?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP);
    `,
    [
      dataset,
      s3Key,
      etag ?? null,
      fileSize ?? null,
      lastModified ?? null,
      schema ? JSON.stringify(schema) : null,
    ],
  );
}

export async function getCachedSchema(dataset, s3Key) {
  ensureDatabase();

  const rows = await runQuery(
    db,
    `
    SELECT schema_json
    FROM crawler_files
    WHERE dataset = ?
      AND s3_key = ?
      AND schema_json IS NOT NULL;
    `,
    [dataset, s3Key],
  );

  if (!rows[0]) return null;

  return JSON.parse(rows[0].schema_json);
}

export async function readParquetSchemaWithDuckDB(s3Path) {
  ensureDatabase();

  const rows = await runQuery(
    db,
    `
    DESCRIBE
    SELECT *
    FROM read_parquet(?);
    `,
    [s3Path],
  );

  return rows.map((column) => ({
    Name: column.column_name,
    Type: duckdbTypeToGlueType(column.column_type),
  }));
}

export async function closeCache() {
  if (!db) return;

  await new Promise((resolve, reject) => {
    db.close((error) => {
      if (error) {
        reject(error);
        return;
      }

      db = null;
      resolve();
    });
  });
}

function ensureDatabase() {
  if (!db) {
    throw new Error('DuckDB cache is not open. Call openCache() first.');
  }
}

function runQuery(database, sql, parameters = []) {
  return new Promise((resolve, reject) => {
    database.all(sql, ...parameters, (error, rows) => {
      if (error) {
        reject(error);
        return;
      }

      resolve(rows);
    });
  });
}

function duckdbTypeToGlueType(type) {
  const normalized = type.toUpperCase();

  if (normalized === 'BOOLEAN') return 'boolean';

  if (
    normalized === 'TINYINT' ||
    normalized === 'SMALLINT' ||
    normalized === 'INTEGER'
  ) {
    return 'int';
  }

  if (
    normalized === 'BIGINT' ||
    normalized === 'HUGEINT'
  ) {
    return 'bigint';
  }

  if (normalized === 'FLOAT') return 'float';
  if (normalized === 'DOUBLE') return 'double';

  if (normalized === 'DATE') return 'date';

  if (normalized.startsWith('TIMESTAMP')) {
    return 'timestamp';
  }

  if (normalized.startsWith('DECIMAL')) {
    return normalized.toLowerCase();
  }

  if (normalized === 'BLOB') {
    return 'binary';
  }

  return 'string';
}