'use strict';

const { validateSchema, validateRow, validateBatch, TYPE_VALIDATORS } = require('../src/services/validator');

describe('Data Validation', () => {
  describe('Type Validators', () => {
    test('INT validator', () => {
      expect(TYPE_VALIDATORS.INT(42)).toBe(true);
      expect(TYPE_VALIDATORS.INT(0)).toBe(true);
      expect(TYPE_VALIDATORS.INT(-5)).toBe(true);
      expect(TYPE_VALIDATORS.INT('123')).toBe(true);
      expect(TYPE_VALIDATORS.INT(3.14)).toBe(false);
      expect(TYPE_VALIDATORS.INT('abc')).toBe(false);
      expect(TYPE_VALIDATORS.INT(null)).toBe(false);
      expect(TYPE_VALIDATORS.INT(undefined)).toBe(false);
    });

    test('STRING validator', () => {
      expect(TYPE_VALIDATORS.STRING('hello')).toBe(true);
      expect(TYPE_VALIDATORS.STRING('')).toBe(true);
      expect(TYPE_VALIDATORS.STRING(123)).toBe(true);
      expect(TYPE_VALIDATORS.STRING(null)).toBe(false);
      expect(TYPE_VALIDATORS.STRING(undefined)).toBe(false);
    });

    test('FLOAT validator', () => {
      expect(TYPE_VALIDATORS.FLOAT(3.14)).toBe(true);
      expect(TYPE_VALIDATORS.FLOAT(42)).toBe(true);
      expect(TYPE_VALIDATORS.FLOAT('3.14')).toBe(true);
      expect(TYPE_VALIDATORS.FLOAT('abc')).toBe(false);
      expect(TYPE_VALIDATORS.FLOAT(null)).toBe(false);
    });

    test('BOOLEAN validator', () => {
      expect(TYPE_VALIDATORS.BOOLEAN(true)).toBe(true);
      expect(TYPE_VALIDATORS.BOOLEAN(false)).toBe(true);
      expect(TYPE_VALIDATORS.BOOLEAN('true')).toBe(true);
      expect(TYPE_VALIDATORS.BOOLEAN('false')).toBe(true);
      expect(TYPE_VALIDATORS.BOOLEAN(0)).toBe(true);
      expect(TYPE_VALIDATORS.BOOLEAN(1)).toBe(true);
      expect(TYPE_VALIDATORS.BOOLEAN('yes')).toBe(true);
      expect(TYPE_VALIDATORS.BOOLEAN('no')).toBe(true);
      expect(TYPE_VALIDATORS.BOOLEAN(null)).toBe(false);
    });
  });

  describe('Schema Validation', () => {
    test('validates matching headers', () => {
      const result = validateSchema(['id', 'name', 'age'], { id: 'INT', name: 'STRING', age: 'INT' });
      expect(result.valid).toBe(true);
      expect(result.missingColumns).toEqual([]);
    });

    test('detects missing columns', () => {
      const result = validateSchema(['id', 'name'], { id: 'INT', name: 'STRING', email: 'STRING' });
      expect(result.valid).toBe(false);
      expect(result.missingColumns).toContain('email');
    });

    test('detects extra columns', () => {
      const result = validateSchema(['id', 'name', 'age', 'extra'], { id: 'INT', name: 'STRING' });
      expect(result.extraColumns).toContain('extra');
      expect(result.extraColumns).toContain('age');
    });

    test('is case-insensitive', () => {
      const result = validateSchema(['ID', 'NAME'], { id: 'INT', name: 'STRING' });
      expect(result.valid).toBe(true);
    });
  });

  describe('Row Validation', () => {
    const schema = { id: 'INT', name: 'STRING', score: 'FLOAT' };

    test('validates correct row', () => {
      const result = validateRow({ id: 1, name: 'Yash', score: 95.5 }, schema);
      expect(result.valid).toBe(true);
      expect(result.errors).toEqual([]);
    });

    test('detects type mismatch', () => {
      const result = validateRow({ id: 'not_a_number', name: 'Yash', score: 95.5 }, schema);
      expect(result.valid).toBe(false);
      expect(result.errors[0].type).toBe('TYPE_MISMATCH');
      expect(result.errors[0].column).toBe('id');
    });

    test('detects missing fields', () => {
      const result = validateRow({ id: 1 }, schema);
      expect(result.valid).toBe(false);
      const missingErrors = result.errors.filter(e => e.type === 'MISSING_FIELD');
      expect(missingErrors).toHaveLength(2); // name and score
    });

    test('coerces string numbers to integers', () => {
      const result = validateRow({ id: '42', name: 'Test', score: '3.14' }, schema);
      expect(result.valid).toBe(true);
      expect(result.coercedRow.id).toBe(42);
      expect(result.coercedRow.score).toBe(3.14);
    });
  });

  describe('Batch Validation', () => {
    const schema = { id: 'INT', name: 'STRING' };

    test('validates batch of correct rows', () => {
      const rows = [
        { id: 1, name: 'Alice' },
        { id: 2, name: 'Bob' },
        { id: 3, name: 'Carol' },
      ];
      const result = validateBatch(rows, schema, { primaryKey: 'id' });
      expect(result.validCount).toBe(3);
      expect(result.invalidCount).toBe(0);
    });

    test('separates valid and invalid rows', () => {
      const rows = [
        { id: 1, name: 'Alice' },
        { id: 'bad', name: 'Invalid' },
        { id: 3, name: 'Carol' },
      ];
      const result = validateBatch(rows, schema, { primaryKey: 'id' });
      expect(result.validCount).toBe(2);
      expect(result.invalidCount).toBe(1);
    });

    test('detects duplicate keys within batch', () => {
      const rows = [
        { id: 1, name: 'Alice' },
        { id: 2, name: 'Bob' },
        { id: 1, name: 'Duplicate' },
      ];
      const result = validateBatch(rows, schema, { primaryKey: 'id' });
      expect(result.duplicateKeys).toHaveLength(1);
      expect(result.duplicateKeys[0].key).toBe(1);
    });

    test('collects errors with row indices', () => {
      const rows = [
        { id: 1, name: 'Alice' },
        { id: 'bad' },
        { id: 3, name: 'Carol' },
      ];
      const result = validateBatch(rows, schema, { primaryKey: 'id' });
      const errorIndices = result.errors.map(e => e.rowIndex);
      expect(errorIndices).toContain(1);
    });
  });
});
