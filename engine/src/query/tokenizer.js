'use strict';

// SQL tokenizer — performs lexical analysis on SQL strings, producing
// a stream of tokens for consumption by the parser.

const TokenType = Object.freeze({
  KEYWORD:      'KEYWORD',
  IDENTIFIER:   'IDENTIFIER',
  NUMBER:       'NUMBER',
  STRING:       'STRING',
  EQUALS:       'EQUALS',
  STAR:         'STAR',
  LPAREN:       'LPAREN',
  RPAREN:       'RPAREN',
  DOT:          'DOT',
  COMMA:        'COMMA',
  SEMICOLON:    'SEMICOLON',
  EOF:          'EOF',
});

const KEYWORDS = new Set([
  'CREATE', 'TABLE', 'INSERT', 'INTO', 'VALUES',
  'SELECT', 'FROM', 'WHERE', 'BETWEEN', 'AND',
  'INT', 'INTEGER', 'STRING', 'VARCHAR', 'TEXT',
  'FLOAT', 'DOUBLE', 'BOOLEAN', 'BOOL',
  'PRIMARY', 'KEY', 'NOT', 'NULL',
  'DELETE', 'UPDATE', 'SET', 'DROP',
  'INDEX', 'UNIQUE', 'ON',
  'JOIN', 'INNER', 'GROUP', 'BY', 'HAVING', 'AS',
  'COUNT', 'SUM', 'AVG', 'MIN', 'MAX',
  'ORDER', 'ASC', 'DESC', 'LIMIT', 'OFFSET',
]);

function createToken(type, value, position, line, column) {
  return { type, value, position, line, column };
}

// Tokenizes a SQL string into an array of typed tokens with position info.
function tokenize(sql) {
  if (typeof sql !== 'string') {
    throw new Error('Tokenizer expects a string input');
  }

  const tokens = [];
  let pos = 0;
  let line = 1;
  let column = 1;
  const len = sql.length;

  while (pos < len) {
    const char = sql[pos];

    // Skip whitespace
    if (/\s/.test(char)) {
      if (char === '\n') {
        line++;
        column = 1;
      } else {
        column++;
      }
      pos++;
      continue;
    }

    // Skip single-line comments
    if (char === '-' && pos + 1 < len && sql[pos + 1] === '-') {
      pos += 2;
      column += 2;
      while (pos < len && sql[pos] !== '\n') {
        pos++;
        column++;
      }
      continue;
    }

    // Numbers (integers and floats)
    if (/[0-9]/.test(char) || (char === '-' && pos + 1 < len && /[0-9]/.test(sql[pos + 1]))) {
      const startPos = pos;
      const startCol = column;
      let numStr = '';

      if (char === '-') {
        numStr += '-';
        pos++;
        column++;
      }

      while (pos < len && /[0-9]/.test(sql[pos])) {
        numStr += sql[pos];
        pos++;
        column++;
      }

      if (pos < len && sql[pos] === '.' && pos + 1 < len && /[0-9]/.test(sql[pos + 1])) {
        numStr += '.';
        pos++;
        column++;
        while (pos < len && /[0-9]/.test(sql[pos])) {
          numStr += sql[pos];
          pos++;
          column++;
        }
        tokens.push(createToken(TokenType.NUMBER, parseFloat(numStr), startPos, line, startCol));
      } else {
        tokens.push(createToken(TokenType.NUMBER, parseInt(numStr, 10), startPos, line, startCol));
      }
      continue;
    }

    // Single-quoted strings
    if (char === "'") {
      const startPos = pos;
      const startCol = column;
      pos++;
      column++;
      let str = '';
      while (pos < len && sql[pos] !== "'") {
        if (sql[pos] === '\\' && pos + 1 < len) {
          pos++;
          column++;
          switch (sql[pos]) {
            case "'": str += "'"; break;
            case '\\': str += '\\'; break;
            case 'n': str += '\n'; break;
            case 't': str += '\t'; break;
            default: str += sql[pos]; break;
          }
        } else {
          str += sql[pos];
        }
        pos++;
        column++;
      }
      if (pos >= len) {
        throw new Error(`Unterminated string literal at line ${line}, column ${startCol}`);
      }
      pos++;
      column++;
      tokens.push(createToken(TokenType.STRING, str, startPos, line, startCol));
      continue;
    }

    // Double-quoted strings
    if (char === '"') {
      const startPos = pos;
      const startCol = column;
      pos++;
      column++;
      let str = '';
      while (pos < len && sql[pos] !== '"') {
        if (sql[pos] === '\\' && pos + 1 < len) {
          pos++;
          column++;
          switch (sql[pos]) {
            case '"': str += '"'; break;
            case '\\': str += '\\'; break;
            case 'n': str += '\n'; break;
            case 't': str += '\t'; break;
            default: str += sql[pos]; break;
          }
        } else {
          str += sql[pos];
        }
        pos++;
        column++;
      }
      if (pos >= len) {
        throw new Error(`Unterminated string literal at line ${line}, column ${startCol}`);
      }
      pos++;
      column++;
      tokens.push(createToken(TokenType.STRING, str, startPos, line, startCol));
      continue;
    }

    // Identifiers and keywords
    if (/[a-zA-Z_]/.test(char)) {
      const startPos = pos;
      const startCol = column;
      let ident = '';
      while (pos < len && /[a-zA-Z0-9_]/.test(sql[pos])) {
        ident += sql[pos];
        pos++;
        column++;
      }
      const upper = ident.toUpperCase();
      if (KEYWORDS.has(upper)) {
        tokens.push(createToken(TokenType.KEYWORD, upper, startPos, line, startCol));
      } else {
        tokens.push(createToken(TokenType.IDENTIFIER, ident, startPos, line, startCol));
      }
      continue;
    }

    // Single-character tokens
    switch (char) {
      case '(':
        tokens.push(createToken(TokenType.LPAREN, '(', pos, line, column));
        break;
      case ')':
        tokens.push(createToken(TokenType.RPAREN, ')', pos, line, column));
        break;
      case ',':
        tokens.push(createToken(TokenType.COMMA, ',', pos, line, column));
        break;
      case '.':
        tokens.push(createToken(TokenType.DOT, '.', pos, line, column));
        break;
      case ';':
        tokens.push(createToken(TokenType.SEMICOLON, ';', pos, line, column));
        break;
      case '=':
        tokens.push(createToken(TokenType.EQUALS, '=', pos, line, column));
        break;
      case '*':
        tokens.push(createToken(TokenType.STAR, '*', pos, line, column));
        break;
      default:
        throw new Error(`Unexpected character '${char}' at line ${line}, column ${column}`);
    }
    pos++;
    column++;
  }

  tokens.push(createToken(TokenType.EOF, null, pos, line, column));
  return tokens;
}

module.exports = { tokenize, TokenType, KEYWORDS };
