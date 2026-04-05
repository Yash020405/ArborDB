'use strict';

// Query executor — converts parsed AST nodes into engine JSON commands
// according to the interface contract between the query layer and storage engine.

function buildEngineCommand(ast, schemaMap = {}) {
  if (!ast || !ast.type) {
    throw new Error('Invalid AST: missing type');
  }

  switch (ast.type) {
    case 'CREATE_TABLE': return buildCreateTable(ast);
    case 'CREATE_INDEX': return buildCreateIndex(ast);
    case 'INSERT': return buildInsert(ast, schemaMap);
    case 'SELECT': return buildSelect(ast, schemaMap);
    case 'UPDATE': return buildUpdate(ast);
    case 'DELETE': return buildDelete(ast);
    case 'DROP_INDEX': return buildDropIndex(ast);
    case 'DROP_TABLE': return { operation: 'drop_table', table: ast.table };
    default: throw new Error(`Unsupported operation type: ${ast.type}`);
  }
}

function buildCreateIndex(ast) {
  return {
    operation: 'create_index',
    table: ast.table,
    index_name: ast.indexName,
    column: ast.column,
    unique: Boolean(ast.unique),
  };
}

function buildDropIndex(ast) {
  return {
    operation: 'drop_index',
    table: ast.table,
    index_name: ast.indexName,
  };
}

function hasSecondaryIndexOnColumn(secondaryIndexes, column) {
  if (!Array.isArray(secondaryIndexes)) return false;

  return secondaryIndexes.some((idx) => {
    if (typeof idx === 'string') {
      return idx === column;
    }
    if (idx && typeof idx === 'object') {
      return idx.column === column;
    }
    return false;
  });
}

function buildCreateTable(ast) {
  const schema = {};
  const columns = [];
  for (const col of ast.columns) {
    schema[col.name] = col.type;
    columns.push({ name: col.name, type: col.type });
  }
  return {
    operation: 'create_table',
    table: ast.table,
    schema,
    columns,
    primary_key: ast.primaryKey || null,
  };
}

function buildInsert(ast, schemaMap) {
  const tableSchema = schemaMap[ast.table];
  let data = {};
  let key = null;

  if (ast.columns) {
    // Explicit column list: INSERT INTO t (col1, col2) VALUES (v1, v2)
    for (let i = 0; i < ast.columns.length; i++) {
      data[ast.columns[i]] = ast.values[i];
    }
  } else if (tableSchema && tableSchema.columns) {
    // Map positional values to column names from schema
    const colNames = tableSchema.columns.map(c => c.name || c);
    for (let i = 0; i < ast.values.length && i < colNames.length; i++) {
      data[colNames[i]] = ast.values[i];
    }
  } else {
    // No schema available, use generic column names
    for (let i = 0; i < ast.values.length; i++) {
      data[`col_${i}`] = ast.values[i];
    }
  }

  // Determine the primary key value
  if (tableSchema && tableSchema.primaryKey) {
    key = data[tableSchema.primaryKey];
  } else if (ast.columns) {
    key = ast.values[0];
  } else {
    key = ast.values[0];
  }

  return { operation: 'insert', table: ast.table, key, data };
}

function buildSelect(ast, schemaMap) {
  const hasJoin = Array.isArray(ast.joins) && ast.joins.length > 0;
  const hasGroupBy = Array.isArray(ast.groupBy) && ast.groupBy.length > 0;
  const hasAggregate = Array.isArray(ast.columns) && ast.columns.some(
    (col) => col && typeof col === 'object' && col.type === 'AGGREGATE'
  );
  const hasOrder = Array.isArray(ast.orderBy) && ast.orderBy.length > 0;
  const hasPagination = Number.isInteger(ast.limit) || Number.isInteger(ast.offset);

  if (hasJoin || hasGroupBy || hasAggregate || ast.having || hasOrder || hasPagination) {
    return {
      operation: 'select_advanced',
      ast,
    };
  }

  if (!ast.condition) {
    return { operation: 'full_scan', table: ast.table };
  }

  const { condition } = ast;

  switch (condition.type) {
    case 'EQUALS': {
      const tableSchema = schemaMap[ast.table];
      const isPrimaryKey = !tableSchema ||
        !tableSchema.primaryKey ||
        tableSchema.primaryKey === condition.column;

      if (isPrimaryKey) {
        return { operation: 'search', table: ast.table, key: condition.value };
      }

      // Use secondary index lookup only when metadata confirms the index exists.
      if (!tableSchema || !Array.isArray(tableSchema.secondaryIndexes)) {
        return {
          operation: 'full_scan',
          table: ast.table,
          filter: { column: condition.column, operator: '=', value: condition.value },
        };
      }

      if (!hasSecondaryIndexOnColumn(tableSchema.secondaryIndexes, condition.column)) {
        return {
          operation: 'full_scan',
          table: ast.table,
          filter: { column: condition.column, operator: '=', value: condition.value },
        };
      }

      return {
        operation: 'search_by_column',
        table: ast.table,
        column: condition.column,
        value: condition.value,
      };
    }

    case 'BETWEEN': {
      const tableSchema = schemaMap[ast.table];
      const isPrimaryKey = !tableSchema ||
        !tableSchema.primaryKey ||
        tableSchema.primaryKey === condition.column;

      if (isPrimaryKey) {
        return { operation: 'range', table: ast.table, start: condition.start, end: condition.end };
      }

      return {
        operation: 'full_scan',
        table: ast.table,
        filter: {
          column: condition.column,
          operator: 'BETWEEN',
          start: condition.start,
          end: condition.end,
        },
      };
    }

    default:
      throw new Error(`Unsupported condition type: ${condition.type}`);
  }
}

function buildUpdate(ast) {
  const result = {
    operation: 'update',
    table: ast.table,
    column: ast.column,
    value: ast.value,
  };
  
  if (ast.condition) {
    result.filter = buildFilterFromCondition(ast.condition);
  }
  
  return result;
}

function buildDelete(ast) {
  const result = {
    operation: 'delete',
    table: ast.table,
  };

  if (ast.condition) {
    result.filter = buildFilterFromCondition(ast.condition);
  }

  return result;
}

function buildFilterFromCondition(condition) {
  if (condition.type === 'EQUALS') {
    return {
      column: condition.column,
      operator: '=',
      value: condition.value,
    };
  }

  if (condition.type === 'BETWEEN') {
    return {
      column: condition.column,
      operator: 'BETWEEN',
      start: condition.start,
      end: condition.end,
    };
  }

  throw new Error(`Unsupported condition type in filter: ${condition.type}`);
}

module.exports = { buildEngineCommand };
