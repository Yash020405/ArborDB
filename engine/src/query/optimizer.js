'use strict';

// Basic query optimizer — determines execution strategy based on
// available indexes. Picks the cheapest path: primary key lookup,
// secondary index, or full scan.

function optimize(ast, tableMetadata = null) {
  if (!ast || ast.type !== 'SELECT') {
    return { ...ast, optimizationHint: { strategy: 'direct', reason: 'Non-SELECT query' } };
  }

  const hasAdvancedFeatures =
    (Array.isArray(ast.joins) && ast.joins.length > 0) ||
    (Array.isArray(ast.groupBy) && ast.groupBy.length > 0) ||
    (Array.isArray(ast.columns) && ast.columns.some((col) => col && typeof col === 'object' && col.type === 'AGGREGATE')) ||
    Boolean(ast.having) ||
    (Array.isArray(ast.orderBy) && ast.orderBy.length > 0) ||
    Number.isInteger(ast.limit) ||
    Number.isInteger(ast.offset);

  if (hasAdvancedFeatures) {
    return {
      ...ast,
      optimizationHint: {
        strategy: 'advanced_select',
        reason: 'Join/group-by/aggregate query executed via advanced planner',
        estimatedCost: 'high',
      },
    };
  }

  if (!ast.condition) {
    return {
      ...ast,
      optimizationHint: {
        strategy: 'full_scan',
        reason: 'No WHERE clause, full table scan required',
        estimatedCost: 'high',
      },
    };
  }

  const { condition } = ast;

  if (!tableMetadata) {
    return {
      ...ast,
      optimizationHint: {
        strategy: condition.type === 'EQUALS' ? 'index_lookup' : 'index_range',
        reason: 'No table metadata, assuming primary key index',
        estimatedCost: 'low',
      },
    };
  }

  const { primaryKey, secondaryIndexes = [] } = tableMetadata;
  const hasSecondaryIndex = (column) => {
    if (!Array.isArray(secondaryIndexes)) return false;
    return secondaryIndexes.some((idx) => {
      if (typeof idx === 'string') return idx === column;
      if (idx && typeof idx === 'object') return idx.column === column;
      return false;
    });
  };

  if (condition.type === 'EQUALS') {
    if (condition.column === primaryKey) {
      return {
        ...ast,
        optimizationHint: {
          strategy: 'primary_key_lookup',
          indexUsed: 'primary',
          column: condition.column,
          reason: `Direct B+ Tree lookup on primary key '${primaryKey}'`,
          estimatedCost: 'very_low',
        },
      };
    }

    if (hasSecondaryIndex(condition.column)) {
      return {
        ...ast,
        optimizationHint: {
          strategy: 'secondary_index_lookup',
          indexUsed: 'secondary',
          column: condition.column,
          reason: `Secondary index lookup on column '${condition.column}'`,
          estimatedCost: 'low',
        },
      };
    }

    return {
      ...ast,
      optimizationHint: {
        strategy: 'full_scan_filter',
        reason: `No index on column '${condition.column}', full scan with filter`,
        estimatedCost: 'high',
        suggestion: `Consider creating a secondary index on '${condition.column}'`,
      },
    };
  }

  if (condition.type === 'BETWEEN') {
    if (condition.column === primaryKey) {
      return {
        ...ast,
        optimizationHint: {
          strategy: 'primary_key_range',
          indexUsed: 'primary',
          column: condition.column,
          reason: `B+ Tree range scan on primary key '${primaryKey}'`,
          estimatedCost: 'low',
        },
      };
    }

    return {
      ...ast,
      optimizationHint: {
        strategy: 'full_scan_filter',
        reason: hasSecondaryIndex(condition.column)
          ? `Secondary index range scans are not supported yet for '${condition.column}', using full scan with range filter`
          : `No index on column '${condition.column}', full scan with range filter`,
        estimatedCost: 'high',
        suggestion: `Consider creating a secondary index on '${condition.column}'`,
      },
    };
  }

  return {
    ...ast,
    optimizationHint: {
      strategy: 'full_scan',
      reason: 'Unknown condition type, defaulting to full scan',
      estimatedCost: 'high',
    },
  };
}

module.exports = { optimize };
