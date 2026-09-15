import { parquetMetadataAsync, parquetSchema } from 'hyparquet';

// Convert common Parquet types to Glue/Athena types. Complex columns are
// represented as strings in this easy-to-read first version.
export async function readParquetSchema(fileBuffer) {
  const metadata = await parquetMetadataAsync(fileBuffer);
  const root = parquetSchema(metadata);
  return (root.children ?? []).map((node) => ({
    Name: node.element.name,
    Type: node.children.length ? 'string' : glueType(node.element),
  }));
}mi 

function glueType(element) {
  const logical = element.logical_type ?? {};
  const logicalType = logical.type;
  const converted = element.converted_type;

  if (logicalType === 'STRING' || logicalType === 'ENUM' || logicalType === 'JSON' ||
      logicalType === 'BSON' || logicalType === 'UUID' ||
      ['UTF8', 'ENUM', 'JSON', 'BSON'].includes(converted)) return 'string';
  if (logicalType === 'DATE' || converted === 'DATE') return 'date';
  if (logicalType === 'TIMESTAMP' || ['TIMESTAMP_MILLIS', 'TIMESTAMP_MICROS'].includes(converted)) return 'timestamp';
  if (logicalType === 'DECIMAL' || converted === 'DECIMAL') {
    const precision = logical.precision ?? element.precision;
    const scale = logical.scale ?? element.scale ?? 0;
    return precision ? `decimal(${precision},${scale})` : 'string';
  }
  if (logicalType === 'INTEGER') return integerLogicalType(logical, element.type);
  switch (element.type) {
    case 'BOOLEAN': return 'boolean';
    case 'INT32': return 'int';
    case 'INT64': return 'bigint';
    case 'INT96': return 'timestamp';
    case 'FLOAT': return 'float';
    case 'DOUBLE': return 'double';
    case 'BYTE_ARRAY':
    case 'FIXED_LEN_BYTE_ARRAY': return 'binary';
    default: return 'string';
  }
}

function integerLogicalType(logical, physicalType) {
  const bits = logical.bitWidth ?? logical.bit_width;
  const signed = logical.isSigned ?? logical.is_signed;
  if (signed === false) return physicalType === 'INT64' ? 'decimal(20,0)' : 'bigint';
  if (bits <= 8) return 'tinyint';
  if (bits <= 16) return 'smallint';
  if (bits <= 32) return 'int';
  return 'bigint';
}
