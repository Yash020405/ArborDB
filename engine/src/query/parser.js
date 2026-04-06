'use strict';

// SQL parser — recursive-descent parser that consumes tokens and produces an AST.
// Supports table/index DDL, DML, joins, and aggregate/group-by query forms.

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
      ast = this.parseCreate();
    } else if (this.checkKeyword('INSERT')) {
      ast = this.parseInsert();
    } else if (this.checkKeyword('SELECT')) {
      ast = this.parseSelect();
    } else if (this.checkKeyword('UPDATE')) {
      ast = this.parseUpdate();
    } else if (this.checkKeyword('DELETE')) {
      ast = this.parseDelete();
    } else if (this.checkKeyword('DROP')) {
      ast = this.parseDrop();
    } else {
      throw this._error(
        `Unexpected token '${token.value}'. Expected CREATE, INSERT, SELECT, UPDATE, DELETE, or DROP`,
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

  // CREATE TABLE ... | CREATE [UNIQUE] INDEX ...
  parseCreate() {
    this.expect(TokenType.KEYWORD, 'CREATE');

    let unique = false;
    if (this.checkKeyword('UNIQUE')) {
      this.advance();
      unique = true;
    }

    if (this.checkKeyword('TABLE')) {
      if (unique) {
        throw this._error("UNIQUE is only supported for CREATE INDEX", this.peek());
      }
      return this.parseCreateTableAfterCreate();
    }

    if (this.checkKeyword('INDEX')) {
      return this.parseCreateIndexAfterCreate(unique);
    }

    throw this._error("Expected 'TABLE' or 'INDEX' after CREATE", this.peek());
  }

  // CREATE TABLE name (col1 TYPE, col2 TYPE, ...)
  parseCreateTableAfterCreate() {
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

        if (this.check(TokenType.LPAREN)) {
          this.advance();
          this.expect(TokenType.NUMBER);
          this.expect(TokenType.RPAREN);
        }

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

  // CREATE [UNIQUE] INDEX index_name ON table_name (column_name)
  parseCreateIndexAfterCreate(unique) {
    this.expect(TokenType.KEYWORD, 'INDEX');
    const indexName = this.expect(TokenType.IDENTIFIER).value;
    this.expect(TokenType.KEYWORD, 'ON');
    const tableName = this.expect(TokenType.IDENTIFIER).value;
    this.expect(TokenType.LPAREN);
    const column = this.expect(TokenType.IDENTIFIER).value;
    this.expect(TokenType.RPAREN);

    return {
      type: 'CREATE_INDEX',
      table: tableName,
      indexName,
      column,
      unique,
    };
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

  // SELECT * | expressions FROM table [JOIN ...] [WHERE ...] [GROUP BY ...] [HAVING ...] [ORDER BY ...] [LIMIT ... [OFFSET ...]]
  parseSelect() {
    this.expect(TokenType.KEYWORD, 'SELECT');

    let columns;
    if (this.check(TokenType.STAR)) {
      this.advance();
      columns = ['*'];
    } else {
      columns = [];
      do {
        columns.push(this.parseSelectExpression());
      } while (this.check(TokenType.COMMA) && this.advance());
    }

    this.expect(TokenType.KEYWORD, 'FROM');
    const from = this.parseTableRef();
    const tableName = from.table;

    const ast = { type: 'SELECT', table: tableName, columns };
    if (from.alias) {
      ast.fromAlias = from.alias;
    }

    const joins = [];
    while (this.checkKeyword('INNER') || this.checkKeyword('JOIN')) {
      if (this.checkKeyword('INNER')) {
        this.advance();
      }

      this.expect(TokenType.KEYWORD, 'JOIN');
      const joinRef = this.parseTableRef();
      this.expect(TokenType.KEYWORD, 'ON');

      const left = this.parseIdentifierPath();
      this.expect(TokenType.EQUALS);
      const right = this.parseIdentifierPath();

      joins.push({
        type: 'INNER',
        table: joinRef.table,
        alias: joinRef.alias || null,
        on: { left, right },
      });
    }

    if (joins.length > 0) {
      ast.joins = joins;
    }

    if (this.checkKeyword('WHERE')) {
      this.advance();
      ast.condition = this.parseCondition();
    }

    if (this.checkKeyword('GROUP')) {
      this.advance();
      this.expect(TokenType.KEYWORD, 'BY');

      ast.groupBy = [];
      do {
        ast.groupBy.push(this.parseIdentifierPath());
      } while (this.check(TokenType.COMMA) && this.advance());
    }

    if (this.checkKeyword('HAVING')) {
      this.advance();
      ast.having = this.parseCondition();
    }

    if (this.checkKeyword('ORDER')) {
      this.advance();
      this.expect(TokenType.KEYWORD, 'BY');

      ast.orderBy = [];
      do {
        const column = this.parseIdentifierPath();
        let direction = 'ASC';

        if (this.checkKeyword('ASC')) {
          this.advance();
          direction = 'ASC';
        } else if (this.checkKeyword('DESC')) {
          this.advance();
          direction = 'DESC';
        }

        ast.orderBy.push({ column, direction });
      } while (this.check(TokenType.COMMA) && this.advance());
    }

    if (this.checkKeyword('LIMIT')) {
      this.advance();
      ast.limit = this.parsePositiveInteger('LIMIT');

      if (this.checkKeyword('OFFSET')) {
        this.advance();
        ast.offset = this.parsePositiveInteger('OFFSET');
      }
    }

    return ast;
  }

  parsePositiveInteger(keyword) {
    const token = this.expect(TokenType.NUMBER);
    if (!Number.isInteger(token.value) || token.value < 0) {
      throw this._error(`${keyword} expects a non-negative integer`, token);
    }
    return token.value;
  }

  parseTableRef() {
    const table = this.expect(TokenType.IDENTIFIER).value;
    let alias = null;

    if (this.check(TokenType.IDENTIFIER)) {
      alias = this.advance().value;
    }

    return { table, alias };
  }

  parseSelectExpression() {
    if (this.checkKeyword('COUNT') || this.checkKeyword('SUM') || this.checkKeyword('AVG') || this.checkKeyword('MIN') || this.checkKeyword('MAX')) {
      const func = this.advance().value;
      this.expect(TokenType.LPAREN);

      let column;
      if (this.check(TokenType.STAR)) {
        this.advance();
        column = '*';
      } else {
        column = this.parseIdentifierPath();
      }

      this.expect(TokenType.RPAREN);

      let alias = null;
      if (this.checkKeyword('AS')) {
        this.advance();
        alias = this.expect(TokenType.IDENTIFIER).value;
      } else if (this.check(TokenType.IDENTIFIER)) {
        alias = this.advance().value;
      }

      return {
        type: 'AGGREGATE',
        func,
        column,
        alias,
      };
    }

    const column = this.parseIdentifierPath();

    if (this.checkKeyword('AS')) {
      this.advance();
      const alias = this.expect(TokenType.IDENTIFIER).value;
      return { type: 'COLUMN', name: column, alias };
    }

    if (this.check(TokenType.IDENTIFIER)) {
      const alias = this.advance().value;
      return { type: 'COLUMN', name: column, alias };
    }

    return column;
  }

  parseIdentifierPath() {
    let name = this.expect(TokenType.IDENTIFIER).value;

    while (this.check(TokenType.DOT)) {
      this.advance();
      name += `.${this.expect(TokenType.IDENTIFIER).value}`;
    }

    return name;
  }

  // UPDATE table SET col = val [WHERE condition]
  parseUpdate() {
    this.expect(TokenType.KEYWORD, 'UPDATE');
    const tableName = this.expect(TokenType.IDENTIFIER).value;

    this.expect(TokenType.KEYWORD, 'SET');
    const column = this.expect(TokenType.IDENTIFIER).value;
    this.expect(TokenType.EQUALS);
    const value = this._parseValue();

    const ast = { type: 'UPDATE', table: tableName, column, value };

    if (this.checkKeyword('WHERE')) {
      this.advance();
      ast.condition = this.parseCondition();
    }

    return ast;
  }

  // DELETE FROM table [WHERE condition]
  parseDelete() {
    this.expect(TokenType.KEYWORD, 'DELETE');
    this.expect(TokenType.KEYWORD, 'FROM');
    const tableName = this.expect(TokenType.IDENTIFIER).value;

    const ast = { type: 'DELETE', table: tableName };

    if (this.checkKeyword('WHERE')) {
      this.advance();
      ast.condition = this.parseCondition();
    }

    return ast;
  }

  // DROP TABLE table | DROP INDEX index_name ON table
  parseDrop() {
    this.expect(TokenType.KEYWORD, 'DROP');

    if (this.checkKeyword('TABLE')) {
      return this.parseDropTableAfterDrop();
    }

    if (this.checkKeyword('INDEX')) {
      return this.parseDropIndexAfterDrop();
    }

    throw this._error("Expected 'TABLE' or 'INDEX' after DROP", this.peek());
  }

  parseDropTableAfterDrop() {
    this.expect(TokenType.KEYWORD, 'TABLE');
    const tableName = this.expect(TokenType.IDENTIFIER).value;

    return { type: 'DROP_TABLE', table: tableName };
  }

  parseDropIndexAfterDrop() {
    this.expect(TokenType.KEYWORD, 'INDEX');
    const indexName = this.expect(TokenType.IDENTIFIER).value;
    this.expect(TokenType.KEYWORD, 'ON');
    const tableName = this.expect(TokenType.IDENTIFIER).value;

    return {
      type: 'DROP_INDEX',
      table: tableName,
      indexName,
    };
  }

  // WHERE col = value | col != value | col < value | col <= value | col > value | col >= value | col BETWEEN val AND val
  parseCondition() {
    const column = this.parseIdentifierPath();

    if (this.check(TokenType.EQUALS)) {
      this.advance();
      const value = this._parseValue();
      return { type: 'EQUALS', column, value };
    }

    if (this.check(TokenType.NEQ)) {
      this.advance();
      const value = this._parseValue();
      return { type: 'NEQ', column, value };
    }

    if (this.check(TokenType.LT)) {
      this.advance();
      const value = this._parseValue();
      return { type: 'LT', column, value };
    }

    if (this.check(TokenType.LTE)) {
      this.advance();
      const value = this._parseValue();
      return { type: 'LTE', column, value };
    }

    if (this.check(TokenType.GT)) {
      this.advance();
      const value = this._parseValue();
      return { type: 'GT', column, value };
    }

    if (this.check(TokenType.GTE)) {
      this.advance();
      const value = this._parseValue();
      return { type: 'GTE', column, value };
    }

    if (this.checkKeyword('BETWEEN')) {
      this.advance();
      const start = this._parseValue();
      this.expect(TokenType.KEYWORD, 'AND');
      const end = this._parseValue();
      return { type: 'BETWEEN', column, start, end };
    }

    throw this._error(
      `Expected a comparison operator or 'BETWEEN' after column '${column}', got '${this.peek().value}'`,
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
    if (token.type === TokenType.KEYWORD && (token.value === 'TRUE' || token.value === 'FALSE')) {
      this.advance();
      return token.value === 'TRUE';
    }
    if (token.type === TokenType.IDENTIFIER) {
      this.advance();
      if (typeof token.value === 'string') {
        const lowered = token.value.toLowerCase();
        if (lowered === 'true') return true;
        if (lowered === 'false') return false;
      }
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
