function applyExcludeColumns(columns, excludeColumns = [], datasetName = "") {
  const excluded = new Set(excludeColumns.map((column) => column.toLowerCase()));

  const retainedColumns = columns.filter(
    (column) => !excluded.has(column.name.toLowerCase())
  );

  const excludedFound = columns
    .filter((column) => excluded.has(column.name.toLowerCase()))
    .map((column) => column.name);

  const excludedNotFound = excludeColumns.filter(
    (column) =>
      !columns.some(
        (schemaColumn) =>
          schemaColumn.name.toLowerCase() === column.toLowerCase()
      )
  );

  console.log(
    `[ColumnFilter] ${datasetName}: ${retainedColumns.length}/${columns.length} columns retained.`
  );

  if (excludedFound.length > 0) {
    console.log(
      `[ColumnFilter] ${datasetName}: excluded columns: ${excludedFound.join(", ")}`
    );
  }

  if (excludedNotFound.length > 0) {
    console.warn(
      `[ColumnFilter] ${datasetName}: configured columns not found in Glue schema: ${excludedNotFound.join(", ")}`
    );
  }

  return retainedColumns;
}

module.exports = {
  applyExcludeColumns,
};