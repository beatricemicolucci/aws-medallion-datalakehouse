// Import the required AWS SDK clients and commands for Node.js
import {
  CreatePartitionCommand,
  CreateTableCommand,
  GetPartitionCommand,
  GetTableCommand,
  UpdateTableCommand,
  paginateGetPartitions,
} from '@aws-sdk/client-glue';

// This function returns a an object that describes how data is stored in S3 in Parquet format.
function parquetStorage(location, columns) {
  return {
    Location: location,
    Columns: columns,
    InputFormat: 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat',
    OutputFormat: 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat',
    SerdeInfo: { SerializationLibrary: 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' },
  };
}

// Async means that some operations require communication with AWS services.
// It takes a Glue client, database Glue, name of the table, columns, names of partitions.
export async function saveTable(glue, database, name, location, columns, partitionNames) {
  const tableInput = {
    Name: name,
    TableType: 'EXTERNAL_TABLE', // This is a table of metadata that points to data stored in S3
    Parameters: { classification: 'parquet', EXTERNAL: 'TRUE' },
    StorageDescriptor: parquetStorage(location, columns),
    PartitionKeys: partitionNames.map((Name) => ({ Name, Type: 'string' })),
  };

  try {
    await glue.send(new GetTableCommand({ DatabaseName: database, Name: name }));
    await glue.send(new UpdateTableCommand({ DatabaseName: database, TableInput: tableInput, SkipArchive: true }));
    return 'updated';
  } catch (error) {
    if (error.name !== 'EntityNotFoundException') throw error;
    await glue.send(new CreateTableCommand({ DatabaseName: database, TableInput: tableInput }));
    return 'created';
  }
}

export async function savePartitions(glue, database, tableName, columns, partitions) {
  // GetPartitions is paginated: there could be more than 1,000 partitions.
  const existing = new Set();
  const pages = paginateGetPartitions(
    { client: glue, pageSize: 1000 },
    { DatabaseName: database, TableName: tableName },
  );
  for await (const page of pages) {
    for (const partition of page.Partitions ?? []) existing.add(JSON.stringify(partition.Values));
  }

  let created = 0;
  for (const partition of partitions) {
    const id = JSON.stringify(partition.values);
    if (existing.has(id)) continue;

    // This extra check also makes the crawler safe if another execution adds
    // the same partition after the listing above.
    try {
      await glue.send(new GetPartitionCommand({
        DatabaseName: database,
        TableName: tableName,
        PartitionValues: partition.values,
      }));
      continue;
    } catch (error) {
      if (error.name !== 'EntityNotFoundException') throw error;
    }

    await glue.send(new CreatePartitionCommand({
      DatabaseName: database,
      TableName: tableName,
      PartitionInput: {
        Values: partition.values,
        StorageDescriptor: parquetStorage(partition.location, columns),
      },
    }));
    created += 1;
  }
  return created;
}
