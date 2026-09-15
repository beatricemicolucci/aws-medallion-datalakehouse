// Import the required AWS SDK clients and commands for Node.js
import {
  BatchCreatePartitionCommand,
  CreateTableCommand,
  GetTableCommand,
  UpdateTableCommand,
  paginateGetPartitions,
} from '@aws-sdk/client-glue';

// This function returns an object that describes how data is stored in S3 in Parquet format.
function parquetStorage(location, columns) {
  return {
    Location: location,
    Columns: columns,
    InputFormat: 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat',
    OutputFormat: 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat',
    SerdeInfo: {
      SerializationLibrary: 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe',
    },
  };
}
// Async means that some operations require communication with AWS services.
// It takes a Glue client, database Glue, name of the table, columns, names of partitions.
export async function saveTable(
  glue,
  database,
  name,
  location,
  columns,
  partitionNames,
) {
  const tableInput = {
    Name: name,
    TableType: 'EXTERNAL_TABLE', // This is a table of metadata that points to data stored in S3
    Parameters: {
      classification: 'parquet',
      EXTERNAL: 'TRUE',
    },
    StorageDescriptor: parquetStorage(location, columns),
    PartitionKeys: partitionNames.map((Name) => ({
      Name,
      Type: 'string',
    })),
  };

  try {
    await glue.send(
      new GetTableCommand({
        DatabaseName: database,
        Name: name,
      }),
    );

    await glue.send(
      new UpdateTableCommand({
        DatabaseName: database,
        TableInput: tableInput,
        SkipArchive: true,
      }),
    );

    return 'updated';
  } catch (error) {
    if (error.name !== 'EntityNotFoundException') {
      throw error;
    }

    await glue.send(
      new CreateTableCommand({
        DatabaseName: database,
        TableInput: tableInput,
      }),
    );

    return 'created';
  }
}

export async function savePartitions(
  glue,
  database,
  tableName,
  columns,
  partitions,
) {
  // GetPartitions is paginated: there could be more than 1,000 partitions.
  const existing = new Set();

  const pages = paginateGetPartitions(
    { client: glue, pageSize: 1000 },
    {
      DatabaseName: database,
      TableName: tableName,
    },
  );

  for await (const page of pages) {
    for (const partition of page.Partitions ?? []) {
      existing.add(JSON.stringify(partition.Values));
    }
  }

  // Keep only partitions that do not already exist in Glue.
  const newPartitions = partitions.filter((partition) => {
    const id = JSON.stringify(partition.values);
    return !existing.has(id);
  });

  if (!newPartitions.length) {
    return 0;
  }

  let created = 0;

  // Glue BatchCreatePartition supports up to 100 partitions per request.
  const batchSize = 100;

  for (let index = 0; index < newPartitions.length; index += batchSize) {
    const batch = newPartitions.slice(index, index + batchSize);

    const inputs = batch.map((partition) => ({
      Values: partition.values,
      StorageDescriptor: parquetStorage(
        partition.location,
        columns,
      ),
    }));

    const response = await glue.send(
      new BatchCreatePartitionCommand({
        DatabaseName: database,
        TableName: tableName,
        PartitionInputList: inputs,
      }),
    );

    // BatchCreatePartition can return errors for individual partitions.
    const errors = response.Errors ?? [];

    if (errors.length > 0) {
      console.error(
        `Batch ${Math.floor(index / batchSize) + 1}: `
        + `${errors.length} partition(s) failed.`,
      );

      for (const error of errors) {
        console.error(
          `Partition error: ${error.ErrorDetail?.ErrorCode ?? 'Unknown'} `
          + `${error.ErrorDetail?.ErrorMessage ?? ''}`,
        );
      }
    }

    created += batch.length - errors.length;

    console.log(
      `Partition batch ${Math.floor(index / batchSize) + 1}: `
      + `${batch.length - errors.length}/${batch.length} created.`,
    );
  }

  return created;
}