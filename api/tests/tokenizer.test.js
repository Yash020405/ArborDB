'use strict';

const { tokenize, TokenType } = require('../../engine/src/query/tokenizer');

describe('SQL Tokenizer', () => {
  describe('Basic Token Types', () => {
    test('tokenizes keywords', () => {
      const tokens = tokenize('SELECT FROM WHERE');
      expect(tokens[0]).toMatchObject({ type: TokenType.KEYWORD, value: 'SELECT' });
      expect(tokens[1]).toMatchObject({ type: TokenType.KEYWORD, value: 'FROM' });
      expect(tokens[2]).toMatchObject({ type: TokenType.KEYWORD, value: 'WHERE' });
      expect(tokens[3]).toMatchObject({ type: TokenType.EOF });
    });

    test('tokenizes identifiers', () => {
      const tokens = tokenize('users my_table col1');
      expect(tokens[0]).toMatchObject({ type: TokenType.IDENTIFIER, value: 'users' });
      expect(tokens[1]).toMatchObject({ type: TokenType.IDENTIFIER, value: 'my_table' });
      expect(tokens[2]).toMatchObject({ type: TokenType.IDENTIFIER, value: 'col1' });
    });

    test('tokenizes integers', () => {
      const tokens = tokenize('42 0 999');
      expect(tokens[0]).toMatchObject({ type: TokenType.NUMBER, value: 42 });
      expect(tokens[1]).toMatchObject({ type: TokenType.NUMBER, value: 0 });
      expect(tokens[2]).toMatchObject({ type: TokenType.NUMBER, value: 999 });
    });

    test('tokenizes floating point numbers', () => {
      const tokens = tokenize('3.14 0.5');
      expect(tokens[0]).toMatchObject({ type: TokenType.NUMBER, value: 3.14 });
      expect(tokens[1]).toMatchObject({ type: TokenType.NUMBER, value: 0.5 });
    });

    test('tokenizes single-quoted strings', () => {
      const tokens = tokenize("'hello' 'world'");
      expect(tokens[0]).toMatchObject({ type: TokenType.STRING, value: 'hello' });
      expect(tokens[1]).toMatchObject({ type: TokenType.STRING, value: 'world' });
    });

    test('tokenizes double-quoted strings', () => {
      const tokens = tokenize('"hello" "world"');
      expect(tokens[0]).toMatchObject({ type: TokenType.STRING, value: 'hello' });
      expect(tokens[1]).toMatchObject({ type: TokenType.STRING, value: 'world' });
    });

    test('tokenizes operators and punctuation', () => {
      const tokens = tokenize('= * ( ) , ;');
      expect(tokens[0]).toMatchObject({ type: TokenType.EQUALS });
      expect(tokens[1]).toMatchObject({ type: TokenType.STAR });
      expect(tokens[2]).toMatchObject({ type: TokenType.LPAREN });
      expect(tokens[3]).toMatchObject({ type: TokenType.RPAREN });
      expect(tokens[4]).toMatchObject({ type: TokenType.COMMA });
      expect(tokens[5]).toMatchObject({ type: TokenType.SEMICOLON });
    });
  });

  describe('Full SQL Statements', () => {
    test('tokenizes CREATE TABLE statement', () => {
      const tokens = tokenize('CREATE TABLE users (id INT, name STRING)');
      const values = tokens.map(t => t.value);
      expect(values).toEqual([
        'CREATE', 'TABLE', 'users', '(', 'id', 'INT', ',', 'name', 'STRING', ')', null
      ]);
    });

    test('tokenizes INSERT statement', () => {
      const tokens = tokenize("INSERT INTO users VALUES (1, 'Yash')");
      const types = tokens.map(t => t.type);
      expect(types).toEqual([
        'KEYWORD', 'KEYWORD', 'IDENTIFIER', 'KEYWORD', 'LPAREN',
        'NUMBER', 'COMMA', 'STRING', 'RPAREN', 'EOF'
      ]);
    });

    test('tokenizes SELECT with WHERE', () => {
      const tokens = tokenize('SELECT * FROM users WHERE id = 1');
      expect(tokens).toHaveLength(9); // 8 tokens + EOF
    });

    test('tokenizes SELECT with BETWEEN', () => {
      const tokens = tokenize('SELECT * FROM users WHERE id BETWEEN 1 AND 10');
      const keywords = tokens.filter(t => t.type === TokenType.KEYWORD).map(t => t.value);
      expect(keywords).toContain('BETWEEN');
      expect(keywords).toContain('AND');
    });
  });

  describe('Edge Cases', () => {
    test('handles empty input', () => {
      const tokens = tokenize('');
      expect(tokens).toEqual([expect.objectContaining({ type: TokenType.EOF })]);
    });

    test('handles whitespace only', () => {
      const tokens = tokenize('   \n\t  ');
      expect(tokens).toEqual([expect.objectContaining({ type: TokenType.EOF })]);
    });

    test('handles SQL comments', () => {
      const tokens = tokenize('SELECT -- this is a comment\n* FROM users');
      const values = tokens.filter(t => t.type !== TokenType.EOF).map(t => t.value);
      expect(values).toEqual(['SELECT', '*', 'FROM', 'users']);
    });

    test('handles escape sequences in strings', () => {
      const tokens = tokenize("'can\\'t stop'");
      expect(tokens[0]).toMatchObject({ type: TokenType.STRING, value: "can't stop" });
    });

    test('tracks line and column positions', () => {
      const tokens = tokenize('SELECT\n  *\nFROM users');
      expect(tokens[0]).toMatchObject({ line: 1, column: 1 }); // SELECT
      expect(tokens[1]).toMatchObject({ line: 2, column: 3 }); // *
      expect(tokens[2]).toMatchObject({ line: 3, column: 1 }); // FROM
    });

    test('is case-insensitive for keywords', () => {
      const tokens = tokenize('select FROM Where');
      expect(tokens[0]).toMatchObject({ type: TokenType.KEYWORD, value: 'SELECT' });
      expect(tokens[1]).toMatchObject({ type: TokenType.KEYWORD, value: 'FROM' });
      expect(tokens[2]).toMatchObject({ type: TokenType.KEYWORD, value: 'WHERE' });
    });
  });

  describe('Error Handling', () => {
    test('throws on unexpected character', () => {
      expect(() => tokenize('SELECT @ FROM')).toThrow('Unexpected character');
    });

    test('throws on unterminated single-quoted string', () => {
      expect(() => tokenize("'unterminated")).toThrow('Unterminated string');
    });

    test('throws on unterminated double-quoted string', () => {
      expect(() => tokenize('"unterminated')).toThrow('Unterminated string');
    });

    test('throws on non-string input', () => {
      expect(() => tokenize(123)).toThrow('Tokenizer expects a string');
      expect(() => tokenize(null)).toThrow('Tokenizer expects a string');
    });
  });
});
