'use strict';

// Query module public API — combines tokenizer, parser, executor, and optimizer.

const { tokenize, TokenType, KEYWORDS } = require('./tokenizer');
const { parse: parseTokens } = require('./parser');
const { buildEngineCommand } = require('./executor');
const { optimize } = require('./optimizer');
const { executeAdvancedSelect } = require('./advanced');

// Parse SQL string into an AST.
function parse(sql) {
  const tokens = tokenize(sql);
  return parseTokens(tokens);
}

// Full pipeline: parse SQL, optimize, and build engine command.
function processQuery(sql, schemaMap = {}, tableMetadata = null) {
  const tokens = tokenize(sql);
  const ast = parseTokens(tokens);

  const meta = tableMetadata || (schemaMap[ast.table] ? {
    primaryKey: schemaMap[ast.table].primaryKey,
    secondaryIndexes: schemaMap[ast.table].secondaryIndexes || schemaMap[ast.table].secondaryIndexDefs || [],
  } : null);

  const optimized = optimize(ast, meta);
  const command = buildEngineCommand(ast, schemaMap);

  return { ast, optimized, command };
}

module.exports = {
  parse,
  processQuery,
  tokenize,
  parseTokens,
  buildEngineCommand,
  optimize,
  executeAdvancedSelect,
  TokenType,
  KEYWORDS,
};
