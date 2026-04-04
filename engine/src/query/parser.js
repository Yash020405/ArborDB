'use strict';

// SQL parser — recursive-descent parser that consumes tokens and produces an AST.
// Supports CREATE TABLE, INSERT INTO, SELECT with WHERE (=, BETWEEN).

const { TokenType } = require('./tokenizer');

class Parser {
  constructor(tokens) {
    this.tokens = tokens;
    this.pos = 0;
  }

  peek() {
    return this.tokens[this.pos];
  }

  advance() {
    const token = this.tokens[this.pos];
    this.pos++;
    return token;
  }

  expect(type, value = null) {
    const token = this.peek();
    if (!token) {
      throw this._error(`Unexpected end of input, expected ${type}${value ? ` '${value}'` : ''}`);
    }
    if (token.type !== type) {
      throw this._error(
        `Expected ${type}${value ? ` '${value}'` : ''}, got ${token.type} '${token.value}'`,
        token
      );
    }
    if (value !== null && token.value !== value) {
      throw this._error(`Expected '${value}', got '${token.value}'`, token);
    }
    return this.advance();
  }

  check(type, value = null) {
    const token = this.peek();
    if (!token || token.type !== type) return false;
    if (value !== null && token.value !== value) return false;
    return true;
  }

  checkKeyword(value) {
    return this.check(TokenType.KEYWORD, value);
  }

  _error(message, token = null) {
    const t = token || this.peek();
    if (t && t.line !== undefined) {
      return new Error(`Parse error at line ${t.line}, column ${t.column}: ${message}`);
    }
    return new Error(`Parse error: ${message}`);
  }

  parse() {
    const token = this.peek();

    if (!token || token.type === TokenType.EOF) {
      throw this._error('Empty query');
    }

    let ast;

    if (this.checkKeyword('CREATE')) {
      ast = this.parseCreateTable();
    } else if (this.checkKeyword('INSERT')) {
      ast = this.parseInsert();
    } else if (this.checkKeyword('SELECT')) {
      ast = this.parseSelect();
    } else {
      throw this._error(
        `Unexpected token '${token.value}'. Expected CREATE, INSERT, or SELECT`,
        token
      );
    }

    if (this.check(TokenType.SEMICOLON)) {
      this.advance();
    }

    if (!this.check(TokenType.EOF)) {
      const remaining = this.peek();
      throw this._error(`Unexpected token '${remaining.value}' after end of statement`, remaining);
    }

    return ast;
  }

  // CREATE TABLE name (col1 TYPE, col2 TYPE, ...)
  parseCreateTable() {
    this.expect(TokenType.KEYWORD, 'CREATE');
    this.expect(TokenType.KEYWORD, 'TABLE');

    const tableToken = this.expect(TokenType.IDENTIFIER);
    const tableName = tableToken.value;

    this.expect(TokenType.LPAREN);

    const columns = [];
    let primaryKey = null;

    do {
      if (this.checkKeyword('PRIMARY')) {
        this.advance();
        this.expect(TokenType.KEYWORD, 'KEY');
        this.expect(TokenType.LPAREN);
        const pkToken = this.expect(TokenType.IDENTIFIER);
        primaryKey = pkToken.value;
        this.expect(TokenType.RPAREN);
      } else {
        const colName = this.expect(TokenType.IDENTIFIER).value;
        const colType = this.expect(TokenType.KEYWORD).value;
        const normalizedType = this._normalizeType(colType);
        const colDef = { name: colName, type: normalizedType };

        if (this.checkKeyword('PRIMARY')) {
          this.advance();
          this.expect(TokenType.KEYWORD, 'KEY');
          primaryKey = colName;
          colDef.primaryKey = true;
        }

        if (this.checkKeyword('NOT')) {
          this.advance();
          this.expect(TokenType.KEYWORD, 'NULL');
          colDef.notNull = true;
        }

        columns.push(colDef);
      }
    } while (this.check(TokenType.COMMA) && this.advance());

    this.expect(TokenType.RPAREN);

    // Default primary key to first column if not specified
    if (!primaryKey && columns.length > 0) {
      primaryKey = columns[0].name;
      columns[0].primaryKey = true;
    }

    return { type: 'CREATE_TABLE', table: tableName, columns, primaryKey };
  }

  // INSERT INTO name VALUES (val1, val2, ...)
  // INSERT INTO name (col1, col2) VALUES (val1, val2)
  parseInsert() {
    this.expect(TokenType.KEYWORD, 'INSERT');
    this.expect(TokenType.KEYWORD, 'INTO');

    const tableName = this.expect(TokenType.IDENTIFIER).value;

    // Optional column list
    let columnNames = null;
    if (this.check(TokenType.LPAREN)) {
      this.advance();
      columnNames = [];
      do {
        columnNames.push(this.expect(TokenType.IDENTIFIER).value);
      } while (this.check(TokenType.COMMA) && this.advance());
      this.expect(TokenType.RPAREN);
    }

    this.expect(TokenType.KEYWORD, 'VALUES');
    this.expect(TokenType.LPAREN);

    const values = [];
    do {
      values.push(this._parseValue());
    } while (this.check(TokenType.COMMA) && this.advance());

    this.expect(TokenType.RPAREN);

    const ast = { type: 'INSERT', table: tableName, values };
    if (columnNames) {
      ast.columns = columnNames;
    }
    return ast;
  }

  // SELECT * | col1, col2 FROM name [WHERE condition]
  parseSelect() {
    this.expect(TokenType.KEYWORD, 'SELECT');

    let columns;
    if (this.check(TokenType.STAR)) {
      this.advance();
      columns = ['*'];
    } else {
      columns = [];
      do {
        columns.push(this.expect(TokenType.IDENTIFIER).value);
      } while (this.check(TokenType.COMMA) && this.advance());
    }

    this.expect(TokenType.KEYWORD, 'FROM');
    const tableName = this.expect(TokenType.IDENTIFIER).value;

    const ast = { type: 'SELECT', table: tableName, columns };

    if (this.checkKeyword('WHERE')) {
      this.advance();
      ast.condition = this.parseCondition();
    }

    return ast;
  }

  // WHERE col = value  |  WHERE col BETWEEN val1 AND val2
  parseCondition() {
    const column = this.expect(TokenType.IDENTIFIER).value;

    if (this.check(TokenType.EQUALS)) {
      this.advance();
      const value = this._parseValue();
      return { type: 'EQUALS', column, value };
    }

    if (this.checkKeyword('BETWEEN')) {
      this.advance();
      const start = this._parseValue();
      this.expect(TokenType.KEYWORD, 'AND');
      const end = this._parseValue();
      return { type: 'BETWEEN', column, start, end };
    }

    throw this._error(
      `Expected '=' or 'BETWEEN' after column '${column}', got '${this.peek().value}'`,
      this.peek()
    );
  }

  _parseValue() {
    const token = this.peek();
    if (token.type === TokenType.NUMBER) {
      this.advance();
      return token.value;
    }
    if (token.type === TokenType.STRING) {
      this.advance();
      return token.value;
    }
    if (token.type === TokenType.IDENTIFIER) {
      this.advance();
      return token.value;
    }
    if (token.type === TokenType.KEYWORD && token.value === 'NULL') {
      this.advance();
      return null;
    }
    throw this._error(
      `Expected a value (number or string), got ${token.type} '${token.value}'`,
      token
    );
  }

  _normalizeType(type) {
    const upper = type.toUpperCase();
    switch (upper) {
      case 'INT': case 'INTEGER': return 'INT';
      case 'STRING': case 'VARCHAR': case 'TEXT': return 'STRING';
      case 'FLOAT': case 'DOUBLE': return 'FLOAT';
      case 'BOOLEAN': case 'BOOL': return 'BOOLEAN';
      default: return upper;
    }
  }
}

function parse(tokens) {
  const parser = new Parser(tokens);
  return parser.parse();
}

module.exports = { parse, Parser };
