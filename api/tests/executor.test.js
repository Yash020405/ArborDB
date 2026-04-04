'use strict';

const { buildEngineCommand } = require('../../engine/src/query/executor');
const { optimize } = require('../../engine/src/query/optimizer');

describe('Query Executor', () => {
  describe('CREATE TABLE', () => {
    test('builds create_table command', () => {
      const ast = {
        type: 'CREATE_TABLE',
        table: 'users',
        columns: [
          { name: 'id', type: 'INT' },
          { name: 'name', type: 'STRING' },
        ],
        primaryKey: 'id',
      };

      const cmd = buildEngineCommand(ast);
      expect(cmd).toEqual({
        operation: 'create_table',
        table: 'users',
        schema: { id: 'INT', name: 'STRING' },
        primary_key: 'id',
      });
    });
  });

  describe('INSERT', () => {
    test('builds insert command with schema', () => {
      const ast = {
        type: 'INSERT',
        table: 'users',
        values: [1, 'Yash'],
      };

      const schemaMap = {
        users: {
          columns: [{ name: 'id' }, { name: 'name' }],
          primaryKey: 'id',
        },
      };

      const cmd = buildEngineCommand(ast, schemaMap);
      expect(cmd).toEqual({
        operation: 'insert',
        table: 'users',
        key: 1,
        data: { id: 1, name: 'Yash' },
      });
    });

    test('builds insert command with explicit columns', () => {
      const ast = {
        type: 'INSERT',
        table: 'users',
        columns: ['name', 'id'],
        values: ['Yash', 2],
      };

      const cmd = buildEngineCommand(ast);
      expect(cmd.data).toEqual({ name: 'Yash', id: 2 });
      expect(cmd.key).toBe('Yash'); // first value when columns provided
    });

    test('builds insert command without schema (fallback)', () => {
      const ast = {
        type: 'INSERT',
        table: 'test',
        values: [42, 'hello'],
      };

      const cmd = buildEngineCommand(ast);
      expect(cmd.operation).toBe('insert');
      expect(cmd.key).toBe(42); // First value as key
    });
  });

  describe('SELECT', () => {
    test('builds full_scan for SELECT without WHERE', () => {
      const ast = { type: 'SELECT', table: 'users', columns: ['*'] };
      const cmd = buildEngineCommand(ast);
      expect(cmd).toEqual({ operation: 'full_scan', table: 'users' });
    });

    test('builds search for WHERE EQUALS on primary key', () => {
      const ast = {
        type: 'SELECT',
        table: 'users',
        columns: ['*'],
        condition: { type: 'EQUALS', column: 'id', value: 1 },
      };

      const schemaMap = { users: { primaryKey: 'id' } };
      const cmd = buildEngineCommand(ast, schemaMap);
      expect(cmd).toEqual({ operation: 'search', table: 'users', key: 1 });
    });

    test('builds full_scan with filter for WHERE EQUALS on non-primary key', () => {
      const ast = {
        type: 'SELECT',
        table: 'users',
        columns: ['*'],
        condition: { type: 'EQUALS', column: 'name', value: 'Yash' },
      };

      const schemaMap = { users: { primaryKey: 'id' } };
      const cmd = buildEngineCommand(ast, schemaMap);
      expect(cmd.operation).toBe('full_scan');
      expect(cmd.filter).toEqual({ column: 'name', operator: '=', value: 'Yash' });
    });

    test('builds range for WHERE BETWEEN', () => {
      const ast = {
        type: 'SELECT',
        table: 'users',
        columns: ['*'],
        condition: { type: 'BETWEEN', column: 'id', start: 1, end: 10 },
      };

      const cmd = buildEngineCommand(ast);
      expect(cmd).toEqual({ operation: 'range', table: 'users', start: 1, end: 10 });
    });
  });

  describe('Error Cases', () => {
    test('throws on null AST', () => {
      expect(() => buildEngineCommand(null)).toThrow('Invalid AST');
    });

    test('throws on unsupported type', () => {
      expect(() => buildEngineCommand({ type: 'DELETE' })).toThrow('Unsupported operation');
    });
  });
});

describe('Query Optimizer', () => {
  test('returns direct strategy for non-SELECT queries', () => {
    const result = optimize({ type: 'INSERT', table: 'test' });
    expect(result.optimizationHint.strategy).toBe('direct');
  });

  test('returns full_scan for SELECT without WHERE', () => {
    const result = optimize({ type: 'SELECT', table: 'users', columns: ['*'] });
    expect(result.optimizationHint.strategy).toBe('full_scan');
    expect(result.optimizationHint.estimatedCost).toBe('high');
  });

  test('returns primary_key_lookup for equality on PK', () => {
    const ast = {
      type: 'SELECT',
      table: 'users',
      columns: ['*'],
      condition: { type: 'EQUALS', column: 'id', value: 1 },
    };
    const meta = { primaryKey: 'id', secondaryIndexes: [] };
    const result = optimize(ast, meta);
    expect(result.optimizationHint.strategy).toBe('primary_key_lookup');
    expect(result.optimizationHint.estimatedCost).toBe('very_low');
  });

  test('returns secondary_index_lookup when index exists', () => {
    const ast = {
      type: 'SELECT',
      table: 'users',
      columns: ['*'],
      condition: { type: 'EQUALS', column: 'name', value: 'Yash' },
    };
    const meta = { primaryKey: 'id', secondaryIndexes: ['name'] };
    const result = optimize(ast, meta);
    expect(result.optimizationHint.strategy).toBe('secondary_index_lookup');
  });

  test('returns full_scan_filter when no index exists', () => {
    const ast = {
      type: 'SELECT',
      table: 'users',
      columns: ['*'],
      condition: { type: 'EQUALS', column: 'email', value: 'test@test.com' },
    };
    const meta = { primaryKey: 'id', secondaryIndexes: ['name'] };
    const result = optimize(ast, meta);
    expect(result.optimizationHint.strategy).toBe('full_scan_filter');
    expect(result.optimizationHint.suggestion).toContain('secondary index');
  });

  test('returns primary_key_range for BETWEEN on PK', () => {
    const ast = {
      type: 'SELECT',
      table: 'users',
      columns: ['*'],
      condition: { type: 'BETWEEN', column: 'id', start: 1, end: 10 },
    };
    const meta = { primaryKey: 'id' };
    const result = optimize(ast, meta);
    expect(result.optimizationHint.strategy).toBe('primary_key_range');
  });
});
