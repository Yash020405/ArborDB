'use strict';

// Data validation — type checking, missing field detection, duplicate key detection.

const TYPE_VALIDATORS = {
  INT: (val) => {
    if (val === null || val === undefined) return false;
    if (typeof val === 'number') return Number.isInteger(val);
    if (typeof val === 'string') return /^-?\d+$/.test(val.trim());
    return false;
  },
  STRING: (val) => val !== null && val !== undefined,
  FLOAT: (val) => {
    if (val === null || val === undefined) return false;
    if (typeof val === 'number') return !isNaN(val);
    if (typeof val === 'string') return !isNaN(parseFloat(val.trim()));
    return false;
  },
  BOOLEAN: (val) => {
    if (typeof val === 'boolean') return true;
    if (typeof val === 'string') {
      return ['true', 'false', '0', '1', 'yes', 'no'].includes(val.toLowerCase().trim());
    }
    if (typeof val === 'number') return val === 0 || val === 1;
    return false;
  },
};

const TYPE_COERCERS = {
  INT: (val) => typeof val === 'number' ? Math.round(val) : parseInt(String(val).trim(), 10),
  STRING: (val) => String(val),
  FLOAT: (val) => typeof val === 'number' ? val : parseFloat(String(val).trim()),
  BOOLEAN: (val) => {
    if (typeof val === 'boolean') return val;
    if (typeof val === 'number') return val !== 0;
    const s = String(val).toLowerCase().trim();
    return s === 'true' || s === '1' || s === 'yes';
  },
};

// Check if file headers match the table schema columns.
function validateSchema(headers, schema) {
  const schemaColumns = Object.keys(schema).map(c => c.toLowerCase());
  const normalizedHeaders = headers.map(h => h.toLowerCase().trim());

  const missingColumns = schemaColumns.filter(col => !normalizedHeaders.includes(col));
  const extraColumns = normalizedHeaders.filter(h => !schemaColumns.includes(h));
  const mappedColumns = normalizedHeaders.filter(h => schemaColumns.includes(h));

  return { valid: missingColumns.length === 0, missingColumns, extraColumns, mappedColumns };
}

// Validate a single row against a table schema, with optional type coercion.
function validateRow(row, schema, options = { coerce: true }) {
  const errors = [];
  const coercedRow = {};

  for (const [colName, colType] of Object.entries(schema)) {
    const key = colName.toLowerCase();
    let value = row[key] !== undefined ? row[key] : row[colName];

    if (value === null || value === undefined || value === '') {
      errors.push({ column: colName, type: 'MISSING_FIELD', message: `Missing required field '${colName}'` });
      continue;
    }

    const normalizedType = colType.toUpperCase();
    const validator = TYPE_VALIDATORS[normalizedType];

    if (!validator) {
      errors.push({ column: colName, type: 'UNKNOWN_TYPE', message: `Unknown column type '${colType}'` });
      continue;
    }

    if (!validator(value)) {
      errors.push({
        column: colName, type: 'TYPE_MISMATCH',
        message: `Value '${value}' is not a valid ${colType}`,
        received: typeof value, expected: colType,
      });
      continue;
    }

    if (options.coerce) {
      const coercer = TYPE_COERCERS[normalizedType];
      coercedRow[colName] = coercer ? coercer(value) : value;
    } else {
      coercedRow[colName] = value;
    }
  }

  return { valid: errors.length === 0, errors, coercedRow };
}

// Validate a batch of rows, collecting errors and detecting duplicate primary keys.
function validateBatch(rows, schema, options = {}) {
  const { primaryKey = null, coerce = true } = options;

  const validRows = [];
  const invalidRows = [];
  const errors = [];
  const seenKeys = new Set();
  const duplicateKeys = [];

  for (let i = 0; i < rows.length; i++) {
    const result = validateRow(rows[i], schema, { coerce });

    if (!result.valid) {
      invalidRows.push({ rowIndex: i, originalRow: rows[i], errors: result.errors });
      errors.push(...result.errors.map(e => ({ ...e, rowIndex: i })));
      continue;
    }

    if (primaryKey) {
      const keyValue = result.coercedRow[primaryKey];
      if (seenKeys.has(keyValue)) {
        duplicateKeys.push({ rowIndex: i, key: keyValue, column: primaryKey });
        errors.push({
          rowIndex: i, column: primaryKey, type: 'DUPLICATE_KEY',
          message: `Duplicate primary key '${keyValue}' at row ${i}`,
        });
        continue;
      }
      seenKeys.add(keyValue);
    }

    validRows.push(result.coercedRow);
  }

  return {
    validRows, invalidRows, duplicateKeys, errors,
    totalProcessed: rows.length, validCount: validRows.length, invalidCount: invalidRows.length,
  };
}

module.exports = { validateSchema, validateRow, validateBatch, TYPE_VALIDATORS, TYPE_COERCERS };
