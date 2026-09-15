import { GlueClient } from '@aws-sdk/client-glue';
import { S3Client } from '@aws-sdk/client-s3';
import {
  closeCache,
  getCachedSchema,
  openCache,
  readParquetSchemaWithDuckDB,
  saveCachedFile,
} from './duckdb.js';
import { savePartitions, saveTable } from './glue.js';
import {
  glueName,
  normalizePrefix,
  parseParquetKey,
} from './partitions.js';
import { listAllObjects } from './s3.js';

const region = process.env.AWS_REGION ?? 'eu-central-1';
const bucket = required('S3_BUCKET');
const database = required('GLUE_DATABASE');
const prefix = normalizePrefix(process.env.S3_PREFIX ?? '');
const dryRun = process.argv.includes('--dry-run');

const s3 = new S3Client({ region });
const glue = new GlueClient({ region });

async function main() {
  await openCache();

  try {
    console.log(`Scanning s3://${bucket}/${prefix}`);

    if (dryRun) {
      console.log(
        'DRY RUN: Glue changes and cache writes are disabled.',
      );
    }

    const objects = await listAllObjects(s3, bucket, prefix);
    const datasets = groupFilesByDataset(objects);

    console.log(`Found ${datasets.size} dataset(s).`);

    for (const [datasetName, dataset] of datasets) {
      const tableName = glueName(datasetName);

      console.log(
        `\nDataset ${datasetName}: ${dataset.files.length} Parquet file(s).`,
      );

      // One file is enough to discover the schema of the dataset.
      const sampleKey = dataset.files[0];
      const metadata = dataset.fileMetadata.get(sampleKey);

      let columns = await getCachedSchema(datasetName, sampleKey);

      if (columns) {
        console.log(`Schema loaded from cache: ${sampleKey}`);
      } else {
        console.log(`Schema not found in cache. Reading: ${sampleKey}`);

        const samplePath = `s3://${bucket}/${sampleKey}`;

        columns = await readParquetSchemaWithDuckDB(samplePath);

        if (dryRun) {
          console.log(
            `DRY RUN: schema would be cached for ${sampleKey}.`,
          );
        } else {
          await saveCachedFile({
            dataset: datasetName,
            s3Key: sampleKey,
            etag: metadata?.etag,
            fileSize: metadata?.fileSize,
            lastModified: metadata?.lastModified,
            schema: columns,
          });

          console.log(`Schema cached for: ${sampleKey}`);
        }
      }

      console.log(
        `Columns: ${columns
          .map((column) => `${column.Name}:${column.Type}`)
          .join(', ')}`,
      );

      const partitionNames = dataset.partitionNames ?? [];
      const tableLocation = `s3://${bucket}/${prefix}${datasetName}/`;

      if (dryRun) {
        console.log(
          `DRY RUN: would create/update Glue table ${database}.${tableName}.`,
        );

        if (!partitionNames.length) {
          console.log('No Hive partitions to register.');
        } else {
          console.log(
            `DRY RUN: would check/register ${dataset.partitions.length} partition(s).`,
          );
        }

        continue;
      }

      const result = await saveTable(
        glue,
        database,
        tableName,
        tableLocation,
        columns,
        partitionNames,
      );

      console.log(`Table ${database}.${tableName} ${result}.`);

      if (!partitionNames.length) {
        console.log('No Hive partitions to register.');
        continue;
      }

      const created = await savePartitions(
        glue,
        database,
        tableName,
        columns,
        dataset.partitions,
      );

      console.log(
        `Partitions: ${created} created, ${
          dataset.partitions.length - created
        } already present.`,
      );
    }
  } finally {
    await closeCache();
  }
}

function groupFilesByDataset(objects) {
  const datasets = new Map();

  for (const object of objects) {
    if (!object.Key?.toLowerCase().endsWith('.parquet')) continue;

    const parsed = parseParquetKey(object.Key, prefix);
    if (!parsed) continue;

    if (!datasets.has(parsed.dataset)) {
      datasets.set(parsed.dataset, {
        files: [],
        fileMetadata: new Map(),
        partitions: [],
        partitionNames: null,
      });
    }

    const dataset = datasets.get(parsed.dataset);

    dataset.files.push(object.Key);

    dataset.fileMetadata.set(object.Key, {
      etag: object.ETag,
      fileSize: object.Size,
      lastModified: object.LastModified,
    });

    if (!parsed.partitions.length) continue;

    const names = parsed.partitions.map((item) => glueName(item.name));

    if (dataset.partitionNames === null) {
      dataset.partitionNames = names;
    }

    if (
      JSON.stringify(names) !== JSON.stringify(dataset.partitionNames)
    ) {
      console.warn(`Skipping incompatible partition path: ${object.Key}`);
      continue;
    }

    const values = parsed.partitions.map((item) => item.value);

    const alreadyFound = dataset.partitions.some(
      (item) => JSON.stringify(item.values) === JSON.stringify(values),
    );

    if (!alreadyFound) {
      dataset.partitions.push({
        values,
        location: `s3://${bucket}/${parsed.partitionLocation}`,
      });
    }
  }

  return datasets;
}

function required(name) {
  if (!process.env[name]) {
    throw new Error(`Missing environment variable: ${name}`);
  }

  return process.env[name];
}

main().catch((error) => {
  console.error('Crawler failed:', error);
  process.exit(1);
});