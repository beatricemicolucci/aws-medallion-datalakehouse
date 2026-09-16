// ANACLIENTI/ingestion_date=2026-09-01/batchid=123/file.parquet
// becomes dataset ANACLIENTI, keys [ingestion_date, batchid], values [...].
export function parseParquetKey(key, prefix) {
  const parts = key.slice(prefix.length).split('/');
  if (parts.length < 2 || !parts.at(-1).toLowerCase().endsWith('.parquet')) return null;

  const dataset = parts[0];
  const directories = parts.slice(1, -1);
  const partitions = [];
  for (const directory of directories) {
    const [name, ...value] = directory.split('=');
    if (name && value.length) partitions.push({ name, value: value.join('=') });
  }
  return { dataset, partitions, partitionLocation: `${prefix}${dataset}/${directories.join('/')}/` };
}

export function normalizePrefix(prefix = '') {
  const clean = prefix.replace(/^\/+|\/+$/g, '');
  return clean ? `${clean}/` : '';
}

// Lowercase names are friendlier to Athena and valid in Glue.
export function glueName(name) {
  return name.toLowerCase().replace(/[^a-z0-9_]/g, '_');
}
