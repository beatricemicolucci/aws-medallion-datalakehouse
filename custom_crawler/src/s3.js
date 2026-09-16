import { GetObjectCommand, paginateListObjectsV2 } from '@aws-sdk/client-s3';

// Discover dataset prefixes first so their object listings can run in parallel.
export async function listAllObjects(s3, bucket, prefix) {
  const startedAt = Date.now();
  const prefixes = await listChildPrefixes(s3, bucket, prefix);

  if (!prefixes.length) {
    const objects = await listObjectsForPrefix(s3, bucket, prefix);
    console.log(
      `S3 listing: ${objects.length} object(s) in ${Date.now() - startedAt} ms.`,
    );
    return objects;
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

  console.log(
    `S3 listing: ${objects.length} object(s) across ${prefixes.length} prefix(es) in ${Date.now() - startedAt} ms.`,
  );
  return objects;
}

async function listChildPrefixes(s3, bucket, prefix) {
  const prefixes = [];
  let pages = 0;
  const paginator = paginateListObjectsV2(
    { client: s3, pageSize: 1000 },
    { Bucket: bucket, Prefix: prefix, Delimiter: '/' },
  );

  for await (const page of paginator) {
    pages += 1;
    prefixes.push(...(page.CommonPrefixes ?? []).map(({ Prefix }) => Prefix));
  }

  console.log(`S3 prefix discovery: ${prefixes.length} prefix(es) in ${pages} page(s).`);
  return prefixes;
}

// S3 returns at most 1,000 objects per response; the paginator retrieves every page.
async function listObjectsForPrefix(s3, bucket, prefix) {
  const objects = [];
  let pages = 0;
  const paginator = paginateListObjectsV2(
    { client: s3, pageSize: 1000 },
    { Bucket: bucket, Prefix: prefix },
  );

  for await (const page of paginator) {
    pages += 1;
    objects.push(...(page.Contents ?? []));
  }

  console.log(`S3 prefix ${prefix}: ${objects.length} object(s) in ${pages} page(s).`);
  return objects;
}

// A didactic first version: download one representative Parquet file in full.
export async function downloadObjectAsArrayBuffer(s3, bucket, key) {
  const response = await s3.send(new GetObjectCommand({ Bucket: bucket, Key: key }));
  const bytes = await response.Body.transformToByteArray();
  return bytes.buffer.slice(bytes.byteOffset, bytes.byteOffset + bytes.byteLength);
}
