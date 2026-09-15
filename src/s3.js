import { GetObjectCommand, paginateListObjectsV2 } from '@aws-sdk/client-s3';

// Discover dataset prefixes first so their object listings can run in parallel.
export async function listAllObjects(s3, bucket, prefix) {
  const prefixes = await listChildPrefixes(s3, bucket, prefix);

  if (!prefixes.length) {
    return listObjectsForPrefix(s3, bucket, prefix);
  }

  const concurrency = Math.max(
    1,
    Number.parseInt(process.env.S3_LIST_CONCURRENCY ?? '8', 10) || 8,
  );
  const objects = [];

  for (let index = 0; index < prefixes.length; index += concurrency) {
    const batch = prefixes.slice(index, index + concurrency);
    const results = await Promise.all(
      batch.map((datasetPrefix) =>
        listObjectsForPrefix(s3, bucket, datasetPrefix),
      ),
    );
    objects.push(...results.flat());
  }

  return objects;
}

async function listChildPrefixes(s3, bucket, prefix) {
  const prefixes = [];
  const paginator = paginateListObjectsV2(
    { client: s3, pageSize: 1000 },
    { Bucket: bucket, Prefix: prefix, Delimiter: '/' },
  );

  for await (const page of paginator) {
    prefixes.push(...(page.CommonPrefixes ?? []).map(({ Prefix }) => Prefix));
  }

  return prefixes;
}

// S3 returns at most 1,000 objects per response; the paginator retrieves every page.
async function listObjectsForPrefix(s3, bucket, prefix) {
  const objects = [];
  const paginator = paginateListObjectsV2(
    { client: s3, pageSize: 1000 },
    { Bucket: bucket, Prefix: prefix },
  );

  for await (const page of paginator) {
    objects.push(...(page.Contents ?? []));
  }

  return objects;
}

// A didactic first version: download one representative Parquet file in full.
export async function downloadObjectAsArrayBuffer(s3, bucket, key) {
  const response = await s3.send(new GetObjectCommand({ Bucket: bucket, Key: key }));
  const bytes = await response.Body.transformToByteArray();
  return bytes.buffer.slice(bytes.byteOffset, bytes.byteOffset + bytes.byteLength);
}
