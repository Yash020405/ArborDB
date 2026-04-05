'use strict';

const { buildEngineCommand } = require('./executor');

function toNumber(value) {
  if (typeof value === 'number' && Number.isFinite(value)) return value;
  if (typeof value === 'string' && value.trim() !== '') {
    const parsed = Number(value);
    if (Number.isFinite(parsed)) return parsed;
  }
  return null;
}

function valuesEqual(left, right) {
  if (left === right) return true;

  const leftNum = toNumber(left);
  const rightNum = toNumber(right);
  if (leftNum !== null && rightNum !== null) {
    return leftNum === rightNum;
  }

  return String(left) === String(right);
}

function compareValues(left, right) {
  const leftNull = left === undefined || left === null;
  const rightNull = right === undefined || right === null;

  if (leftNull && rightNull) return 0;
  if (leftNull) return 1;
  if (rightNull) return -1;

  const leftNum = toNumber(left);
  const rightNum = toNumber(right);
  if (leftNum !== null && rightNum !== null) {
    if (leftNum < rightNum) return -1;
    if (leftNum > rightNum) return 1;
    return 0;
  }

  return String(left).localeCompare(String(right));
}

function betweenValue(value, start, end) {
  const valueNum = toNumber(value);
  const startNum = toNumber(start);
  const endNum = toNumber(end);

  if (valueNum !== null && startNum !== null && endNum !== null) {
    return valueNum >= startNum && valueNum <= endNum;
  }

  const valueStr = String(value);
  const startStr = String(start);
  const endStr = String(end);
  return valueStr >= startStr && valueStr <= endStr;
}

function resolveField(row, field) {
  if (!field || typeof field !== 'string') return undefined;

  if (Object.prototype.hasOwnProperty.call(row, field)) {
    return row[field];
  }

  if (!field.includes('.')) {
    const suffix = `.${field}`;
    const matches = Object.keys(row).filter((key) => key.endsWith(suffix));
    if (matches.length === 1) {
      return row[matches[0]];
    }
  }

  return undefined;
}

function matchesCondition(row, condition) {
  if (!condition) return true;

  const columnValue = resolveField(row, condition.column);
  if (columnValue === undefined) return false;

  if (condition.type === 'EQUALS') {
    return valuesEqual(columnValue, condition.value);
  }

  if (condition.type === 'BETWEEN') {
    return betweenValue(columnValue, condition.start, condition.end);
  }

  return false;
}

function qualifyRow(row, tableName, alias = null) {
  const qualified = {};
  const prefixes = [tableName];

  if (alias && alias !== tableName) {
    prefixes.push(alias);
  }

  for (const [key, value] of Object.entries(row)) {
    if (!Object.prototype.hasOwnProperty.call(qualified, key)) {
      qualified[key] = value;
    }

    for (const prefix of prefixes) {
      qualified[`${prefix}.${key}`] = value;
    }
  }

  return qualified;
}

function mergeRows(left, right) {
  const merged = { ...left };

  for (const [key, value] of Object.entries(right)) {
    // Keep existing unqualified columns from the left side to avoid collisions.
    if (!key.includes('.') && Object.prototype.hasOwnProperty.call(merged, key)) {
      continue;
    }
    merged[key] = value;
  }

  return merged;
}

function outputNameForColumn(columnRef) {
  if (typeof columnRef !== 'string') return 'value';
  const parts = columnRef.split('.');
  return parts[parts.length - 1];
}

function outputNameForAggregate(expr) {
  if (expr.alias) return expr.alias;
  const col = expr.column === '*' ? 'all' : String(expr.column).replace(/\./g, '_');
  return `${String(expr.func).toLowerCase()}_${col}`;
}

function outputNameForExpression(expr) {
  if (typeof expr === 'string') return outputNameForColumn(expr);
  if (!expr || typeof expr !== 'object') return 'value';

  if (expr.type === 'COLUMN') {
    return expr.alias || outputNameForColumn(expr.name);
  }

  if (expr.type === 'AGGREGATE') {
    return outputNameForAggregate(expr);
  }

  return 'value';
}

function hasAggregateExpressions(ast) {
  return Array.isArray(ast.columns) && ast.columns.some(
    (expr) => expr && typeof expr === 'object' && expr.type === 'AGGREGATE'
  );
}

function isAggregateQuery(ast) {
  return hasAggregateExpressions(ast) || (Array.isArray(ast.groupBy) && ast.groupBy.length > 0);
}

function isPushdownCandidate(ast) {
  const hasJoin = Array.isArray(ast.joins) && ast.joins.length > 0;
  return !hasJoin && !isAggregateQuery(ast) && !ast.having;
}

function stripSingleTableQualifier(name, tableName, alias) {
  if (typeof name !== 'string' || !name.includes('.')) {
    return name;
  }

  const parts = name.split('.');
  if (parts.length !== 2) {
    return null;
  }

  const [prefix, column] = parts;
  if (prefix === tableName || (alias && prefix === alias)) {
    return column;
  }

  return null;
}

function buildPushdownAst(ast) {
  const clone = JSON.parse(JSON.stringify(ast));
  delete clone.orderBy;
  delete clone.limit;
  delete clone.offset;

  if (clone.condition && typeof clone.condition.column === 'string') {
    const stripped = stripSingleTableQualifier(clone.condition.column, clone.table, clone.fromAlias || null);
    if (stripped === null) {
      return null;
    }
    clone.condition.column = stripped;
  }

  return clone;
}

function normalizeOrderByForRawRows(orderBy, columns) {
  if (!Array.isArray(orderBy)) return [];

  return orderBy.map((item) => {
    if (!item || typeof item !== 'object' || typeof item.column !== 'string') {
      return item;
    }

    let resolved = item.column;
    if (!resolved.includes('.') && Array.isArray(columns)) {
      for (const expr of columns) {
        if (expr && typeof expr === 'object' && expr.type === 'COLUMN' && expr.alias === resolved) {
          resolved = expr.name;
          break;
        }
      }
    }

    return {
      ...item,
      column: resolved,
    };
  });
}

function applyOrderBy(rows, orderBy) {
  if (!Array.isArray(orderBy) || orderBy.length === 0) {
    return rows;
  }

  const sorted = [...rows];
  sorted.sort((a, b) => {
    for (const item of orderBy) {
      const direction = item.direction === 'DESC' ? -1 : 1;
      const left = resolveField(a, item.column);
      const right = resolveField(b, item.column);
      const cmp = compareValues(left, right);
      if (cmp !== 0) {
        return cmp * direction;
      }
    }
    return 0;
  });

  return sorted;
}

function applyLimitOffset(rows, limit, offset) {
  const start = Number.isInteger(offset) ? Math.max(0, offset) : 0;
  if (!Number.isInteger(limit)) {
    return rows.slice(start);
  }

  const end = start + Math.max(0, limit);
  return rows.slice(start, end);
}

function computeAggregate(func, column, rows) {
  const upper = String(func).toUpperCase();

  if (upper === 'COUNT') {
    if (column === '*') return rows.length;
    return rows.reduce((count, row) => {
      const value = resolveField(row, column);
      return value === undefined || value === null ? count : count + 1;
    }, 0);
  }

  const values = rows
    .map((row) => resolveField(row, column))
    .filter((value) => value !== undefined && value !== null);

  if (upper === 'SUM' || upper === 'AVG') {
    const nums = values
      .map((value) => toNumber(value))
      .filter((value) => value !== null);

    if (nums.length === 0) return null;

    const sum = nums.reduce((acc, value) => acc + value, 0);
    return upper === 'SUM' ? sum : sum / nums.length;
  }

  if (values.length === 0) return null;

  const compare = (a, b) => {
    const aNum = toNumber(a);
    const bNum = toNumber(b);
    if (aNum !== null && bNum !== null) {
      return aNum - bNum;
    }
    return String(a).localeCompare(String(b));
  };

  if (upper === 'MIN') {
    return values.reduce((best, current) => (compare(current, best) < 0 ? current : best));
  }

  if (upper === 'MAX') {
    return values.reduce((best, current) => (compare(current, best) > 0 ? current : best));
  }

  return null;
}

function applyProjection(rows, columns) {
  if (!Array.isArray(columns) || columns.length === 0) {
    return rows;
  }

  if (columns.length === 1 && columns[0] === '*') {
    return rows;
  }

  return rows.map((row) => {
    const projected = {};

    for (const expr of columns) {
      if (typeof expr === 'string') {
        projected[outputNameForColumn(expr)] = resolveField(row, expr);
        continue;
      }

      if (expr && typeof expr === 'object' && expr.type === 'COLUMN') {
        projected[outputNameForExpression(expr)] = resolveField(row, expr.name);
      }
    }

    return projected;
  });
}

function aggregateRows(rows, ast) {
  const columns = Array.isArray(ast.columns) ? ast.columns : ['*'];
  const groupBy = Array.isArray(ast.groupBy) ? ast.groupBy : [];
  const hasAggregate = columns.some((expr) => expr && typeof expr === 'object' && expr.type === 'AGGREGATE');

  if (!hasAggregate && groupBy.length === 0) {
    return applyProjection(rows, columns);
  }

  const groups = new Map();

  if (groupBy.length === 0) {
    groups.set('__all__', rows);
  } else {
    for (const row of rows) {
      const keyValues = groupBy.map((field) => resolveField(row, field));
      const key = JSON.stringify(keyValues);
      if (!groups.has(key)) {
        groups.set(key, []);
      }
      groups.get(key).push(row);
    }
  }

  const result = [];

  for (const groupRows of groups.values()) {
    const rowOut = {};
    const first = groupRows[0] || {};

    for (const expr of columns) {
      if (typeof expr === 'string') {
        rowOut[outputNameForColumn(expr)] = resolveField(first, expr);
        continue;
      }

      if (!expr || typeof expr !== 'object') {
        continue;
      }

      if (expr.type === 'COLUMN') {
        rowOut[outputNameForExpression(expr)] = resolveField(first, expr.name);
      } else if (expr.type === 'AGGREGATE') {
        rowOut[outputNameForExpression(expr)] = computeAggregate(expr.func, expr.column, groupRows);
      }
    }

    result.push(rowOut);
  }

  return result;
}

async function scanTable(tableName, alias, callEngine, metrics) {
  const response = await callEngine({ operation: 'full_scan', table: tableName });

  if (!response || response.status === 'error') {
    const message = response && response.error ? response.error : `Failed to scan table '${tableName}'`;
    throw new Error(message);
  }

  const responseMetrics = response.metrics || {};
  metrics.disk_reads += responseMetrics.disk_reads || 0;
  metrics.nodes_traversed += responseMetrics.nodes_traversed || 0;

  const rows = Array.isArray(response.rows) ? response.rows : [];
  return rows.map((row) => qualifyRow(row, tableName, alias));
}

function mergeMetrics(target, source) {
  const m = source || {};
  target.disk_reads += m.disk_reads || 0;
  target.nodes_traversed += m.nodes_traversed || 0;
}

async function executeAdvancedSelect(ast, callEngine, schemaMap = {}) {
  const startedAt = Date.now();
  const aggregatedMetrics = {
    disk_reads: 0,
    nodes_traversed: 0,
  };

  if (isPushdownCandidate(ast)) {
    const pushdownAst = buildPushdownAst(ast);
    if (pushdownAst) {
      const command = buildEngineCommand(pushdownAst, schemaMap);
      const response = await callEngine(command);

      if (!response || response.status === 'error') {
        const message = response && response.error ? response.error : 'Advanced pushdown execution failed';
        throw new Error(message);
      }

      mergeMetrics(aggregatedMetrics, response.metrics);

      const qualifiedRows = (Array.isArray(response.rows) ? response.rows : []).map(
        (row) => qualifyRow(row, ast.table, ast.fromAlias || null)
      );

      const orderBy = normalizeOrderByForRawRows(ast.orderBy || [], ast.columns || []);
      let rows = applyOrderBy(qualifiedRows, orderBy);
      rows = applyProjection(rows, ast.columns);
      rows = applyLimitOffset(rows, ast.limit, ast.offset);

      return {
        status: 'ok',
        rows,
        error: null,
        affected_rows: 0,
        metrics: {
          time_ms: Date.now() - startedAt,
          disk_reads: aggregatedMetrics.disk_reads,
          nodes_traversed: aggregatedMetrics.nodes_traversed,
        },
      };
    }
  }

  const baseAlias = ast.fromAlias || null;
  let rows = await scanTable(ast.table, baseAlias, callEngine, aggregatedMetrics);

  for (const join of ast.joins || []) {
    const joinRows = await scanTable(join.table, join.alias || null, callEngine, aggregatedMetrics);
    const nextRows = [];

    for (const left of rows) {
      for (const right of joinRows) {
        const merged = mergeRows(left, right);
        const leftValue = resolveField(merged, join.on.left);
        const rightValue = resolveField(merged, join.on.right);

        if (valuesEqual(leftValue, rightValue)) {
          nextRows.push(merged);
        }
      }
    }

    rows = nextRows;
  }

  if (ast.condition) {
    rows = rows.filter((row) => matchesCondition(row, ast.condition));
  }

  if (isAggregateQuery(ast)) {
    rows = aggregateRows(rows, ast);

    if (ast.having) {
      rows = rows.filter((row) => matchesCondition(row, ast.having));
    }

    rows = applyOrderBy(rows, ast.orderBy || []);
  } else {
    const orderBy = normalizeOrderByForRawRows(ast.orderBy || [], ast.columns || []);
    rows = applyOrderBy(rows, orderBy);
    rows = applyProjection(rows, ast.columns);
  }

  rows = applyLimitOffset(rows, ast.limit, ast.offset);

  return {
    status: 'ok',
    rows,
    error: null,
    affected_rows: 0,
    metrics: {
      time_ms: Date.now() - startedAt,
      disk_reads: aggregatedMetrics.disk_reads,
      nodes_traversed: aggregatedMetrics.nodes_traversed,
    },
  };
}

module.exports = { executeAdvancedSelect };
