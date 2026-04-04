'use strict';

// Query executor — converts parsed AST nodes into engine JSON commands
// according to the interface contract between the query layer and storage engine.

function buildEngineCommand(ast, schemaMap = {}) {
  if (!ast || !ast.type) {
    throw new Error('Invalid AST: missing type');
  }

  switch (ast.type) {
    case 'CREATE_TABLE': return buildCreateTable(ast);
    case 'INSERT': return buildInsert(ast, schemaMap);
    case 'SELECT': return buildSelect(ast, schemaMap);
    default: throw new Error(`Unsupported operation type: ${ast.type}`);
  }
}

function buildCreateTable(ast) {
  const schema = {};
  for (const col of ast.columns) {
    schema[col.name] = col.type;
  }
  return {
    operation: 'create_table',
    table: ast.table,
    schema,
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
      // Non-primary key — full scan with filter (secondary index would optimize this)
      return {
        operation: 'full_scan',
        table: ast.table,
        filter: { column: condition.column, operator: '=', value: condition.value },
      };
    }

    case 'BETWEEN': {
      return { operation: 'range', table: ast.table, start: condition.start, end: condition.end };
    }

    default:
      throw new Error(`Unsupported condition type: ${condition.type}`);
  }
}

module.exports = { buildEngineCommand };
