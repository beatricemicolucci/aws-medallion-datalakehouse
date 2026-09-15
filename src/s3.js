import { GetObjectCommand, paginateListObjectsV2 } from '@aws-sdk/client-s3';

// S3 returns at most 1,000 objects per response. The paginator retrieves every page.
export async function listAllObjects(s3, bucket, prefix) {
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
