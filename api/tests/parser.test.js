'use strict';

const { tokenize } = require('../../engine/src/query/tokenizer');
const { parse } = require('../../engine/src/query/parser');

/** Helper: tokenize + parse */
function parseSql(sql) {
  const tokens = tokenize(sql);
  return parse(tokens);
}

describe('SQL Parser', () => {
  describe('CREATE TABLE', () => {
    test('parses basic CREATE TABLE', () => {
      const ast = parseSql('CREATE TABLE users (id INT, name STRING)');
      expect(ast).toEqual({
        type: 'CREATE_TABLE',
        table: 'users',
        columns: [
          { name: 'id', type: 'INT', primaryKey: true },
          { name: 'name', type: 'STRING' },
        ],
        primaryKey: 'id',
      });
    });

    test('parses CREATE TABLE with explicit PRIMARY KEY', () => {
      const ast = parseSql('CREATE TABLE products (id INT PRIMARY KEY, price FLOAT, name STRING)');
      expect(ast.primaryKey).toBe('id');
      expect(ast.columns[0].primaryKey).toBe(true);
      expect(ast.columns).toHaveLength(3);
    });

    test('normalizes column types', () => {
      const ast = parseSql('CREATE TABLE t (a INTEGER, b VARCHAR, c DOUBLE, d BOOL)');
      expect(ast.columns[0].type).toBe('INT');
      expect(ast.columns[1].type).toBe('STRING');
      expect(ast.columns[2].type).toBe('FLOAT');
      expect(ast.columns[3].type).toBe('BOOLEAN');
    });

    test('handles trailing semicolon', () => {
      const ast = parseSql('CREATE TABLE test (id INT);');
      expect(ast.type).toBe('CREATE_TABLE');
      expect(ast.table).toBe('test');
    });

    test('parses CREATE INDEX', () => {
      const ast = parseSql('CREATE INDEX idx_users_name ON users (name)');
      expect(ast).toEqual({
        type: 'CREATE_INDEX',
        table: 'users',
        indexName: 'idx_users_name',
        column: 'name',
        unique: false,
      });
    });

    test('parses CREATE UNIQUE INDEX', () => {
      const ast = parseSql('CREATE UNIQUE INDEX idx_users_email ON users (email)');
      expect(ast).toEqual({
        type: 'CREATE_INDEX',
        table: 'users',
        indexName: 'idx_users_email',
        column: 'email',
        unique: true,
      });
    });
  });

  describe('INSERT', () => {
    test('parses basic INSERT', () => {
      const ast = parseSql("INSERT INTO users VALUES (1, 'Yash')");
      expect(ast).toEqual({
        type: 'INSERT',
        table: 'users',
        values: [1, 'Yash'],
      });
    });

    test('parses INSERT with column names', () => {
      const ast = parseSql("INSERT INTO users (id, name) VALUES (1, 'Yash')");
      expect(ast.type).toBe('INSERT');
      expect(ast.columns).toEqual(['id', 'name']);
      expect(ast.values).toEqual([1, 'Yash']);
    });

    test('parses INSERT with numeric values', () => {
      const ast = parseSql('INSERT INTO scores VALUES (1, 95, 3.14)');
      expect(ast.values).toEqual([1, 95, 3.14]);
    });

    test('parses INSERT with double-quoted strings', () => {
      const ast = parseSql('INSERT INTO users VALUES (1, "Yash")');
      expect(ast.values).toEqual([1, 'Yash']);
    });
  });

  describe('SELECT', () => {
    test('parses SELECT * FROM', () => {
      const ast = parseSql('SELECT * FROM users');
      expect(ast).toEqual({
        type: 'SELECT',
        table: 'users',
        columns: ['*'],
      });
    });

    test('parses SELECT with specific columns', () => {
      const ast = parseSql('SELECT id, name FROM users');
      expect(ast.columns).toEqual(['id', 'name']);
    });

    test('parses SELECT with WHERE EQUALS', () => {
      const ast = parseSql('SELECT * FROM users WHERE id = 1');
      expect(ast.condition).toEqual({
        type: 'EQUALS',
        column: 'id',
        value: 1,
      });
    });

    test('parses SELECT with WHERE EQUALS string value', () => {
      const ast = parseSql("SELECT * FROM users WHERE name = 'Yash'");
      expect(ast.condition).toEqual({
        type: 'EQUALS',
        column: 'name',
        value: 'Yash',
      });
    });

    test('parses SELECT with WHERE BETWEEN', () => {
      const ast = parseSql('SELECT * FROM users WHERE id BETWEEN 1 AND 10');
      expect(ast.condition).toEqual({
        type: 'BETWEEN',
        column: 'id',
        start: 1,
        end: 10,
      });
    });

    test('parses SELECT with trailing semicolon', () => {
      const ast = parseSql('SELECT * FROM users;');
      expect(ast.type).toBe('SELECT');
    });

    test('parses SELECT with INNER JOIN', () => {
      const ast = parseSql(
        'SELECT users.id, orders.amount FROM users INNER JOIN orders ON users.id = orders.user_id WHERE users.id = 1'
      );

      expect(ast.type).toBe('SELECT');
      expect(ast.table).toBe('users');
      expect(ast.columns).toEqual(['users.id', 'orders.amount']);
      expect(ast.joins).toEqual([
        {
          type: 'INNER',
          table: 'orders',
          alias: null,
          on: { left: 'users.id', right: 'orders.user_id' },
        },
      ]);
      expect(ast.condition).toEqual({ type: 'EQUALS', column: 'users.id', value: 1 });
    });

    test('parses SELECT with GROUP BY and HAVING', () => {
      const ast = parseSql(
        'SELECT user_id, COUNT(*) AS orders_count, SUM(amount) AS total_amount FROM orders GROUP BY user_id HAVING orders_count = 2'
      );

      expect(ast.type).toBe('SELECT');
      expect(ast.table).toBe('orders');
      expect(ast.groupBy).toEqual(['user_id']);
      expect(ast.having).toEqual({ type: 'EQUALS', column: 'orders_count', value: 2 });
      expect(ast.columns).toEqual([
        'user_id',
        { type: 'AGGREGATE', func: 'COUNT', column: '*', alias: 'orders_count' },
        { type: 'AGGREGATE', func: 'SUM', column: 'amount', alias: 'total_amount' },
      ]);
    });

    test('parses SELECT with ORDER BY LIMIT OFFSET', () => {
      const ast = parseSql('SELECT id, name FROM users ORDER BY id DESC, name ASC LIMIT 5 OFFSET 10');

      expect(ast.type).toBe('SELECT');
      expect(ast.table).toBe('users');
      expect(ast.orderBy).toEqual([
        { column: 'id', direction: 'DESC' },
        { column: 'name', direction: 'ASC' },
      ]);
      expect(ast.limit).toBe(5);
      expect(ast.offset).toBe(10);
    });

    test('parses aggregate ORDER BY alias', () => {
      const ast = parseSql('SELECT user_id, COUNT(*) AS orders_count FROM orders GROUP BY user_id ORDER BY orders_count DESC');

      expect(ast.orderBy).toEqual([{ column: 'orders_count', direction: 'DESC' }]);
    });
  });

  describe('Error Handling', () => {
    test('throws on empty query', () => {
      expect(() => parseSql('')).toThrow();
    });

    test('parses DROP TABLE statement', () => {
      const ast = parseSql('DROP TABLE users');
      expect(ast).toEqual({ type: 'DROP_TABLE', table: 'users' });
    });

    test('parses DROP INDEX statement', () => {
      const ast = parseSql('DROP INDEX idx_users_name ON users');
      expect(ast).toEqual({
        type: 'DROP_INDEX',
        table: 'users',
        indexName: 'idx_users_name',
      });
    });

    test('parses UPDATE statement with WHERE', () => {
      const ast = parseSql("UPDATE users SET name = 'New' WHERE id = 1");
      expect(ast.type).toBe('UPDATE');
      expect(ast.table).toBe('users');
      expect(ast.column).toBe('name');
      expect(ast.value).toBe('New');
      expect(ast.condition).toEqual({ type: 'EQUALS', column: 'id', value: 1 });
    });

    test('parses DELETE statement with WHERE', () => {
      const ast = parseSql('DELETE FROM users WHERE id BETWEEN 1 AND 10');
      expect(ast.type).toBe('DELETE');
      expect(ast.table).toBe('users');
      expect(ast.condition).toEqual({ type: 'BETWEEN', column: 'id', start: 1, end: 10 });
    });

    test('throws on missing table name', () => {
      expect(() => parseSql('SELECT * FROM')).toThrow();
    });

    test('throws on missing VALUES keyword', () => {
      expect(() => parseSql('INSERT INTO users (1)')).toThrow();
    });

    test('throws on trailing unexpected tokens', () => {
      expect(() => parseSql('SELECT * FROM users alias trailing_token')).toThrow();
    });

    test('throws on invalid WHERE operator', () => {
      expect(() => parseSql('SELECT * FROM users WHERE id LIKE something')).toThrow();
    });

    test('error includes position info', () => {
      try {
        parseSql('SELECT @ FROM users');
      } catch (e) {
        expect(e.message).toContain('line');
      }
    });
  });
});
