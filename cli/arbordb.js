#!/usr/bin/env node
'use strict';

const readline = require('readline');
const path = require('path');
const fs = require('fs');
const chalk = require('chalk');

// Path configs to reuse the backend logic directly (no API call needed)
require('dotenv').config({ path: path.join(__dirname, '../api/.env') });
const engine = require('../api/src/engine');
const { processQuery } = require('../engine/src/query');
const { printTable } = require('./formatter');

const HISTORY_FILE = path.join(process.env.HOME || process.env.USERPROFILE, '.arbordb_history');

console.log(chalk.green('Welcome to the ArborDB monitor.  Commands end with ; or \\g.'));
console.log(chalk.green('Connected to engine: native (C++)'));
console.log(chalk.grey('Type \'.help\' for help.\n'));

const rl = readline.createInterface({
  input: process.stdin,
  output: process.stdout,
  prompt: 'arbordb> ',
  historySize: 1000
});

// Load history
if (fs.existsSync(HISTORY_FILE)) {
  const history = fs.readFileSync(HISTORY_FILE, 'utf8').split('\n').filter(Boolean).reverse();
  rl.history = history;
}

let statementBuffer = '';

function saveHistory() {
  fs.writeFileSync(HISTORY_FILE, rl.history.slice().reverse().join('\n') + '\n');
}

rl.prompt();

rl.on('line', async (line) => {
  const trimmed = line.trim();

  if (trimmed === '') {
    rl.prompt();
    return;
  }

  // Clear current input buffer in monitor mode
  if (trimmed === '\\c') {
    statementBuffer = '';
    rl.setPrompt('arbordb> ');
    rl.prompt();
    return;
  }

  // Handle Meta commands (starting with .) if not part of a multi-line query
  if (statementBuffer === '' && trimmed.startsWith('.')) {
    handleMetaCommand(trimmed);
    rl.prompt();
    return;
  }

  statementBuffer += (statementBuffer ? ' ' : '') + trimmed;

  // If the query ends with semicolon, process it
  if (statementBuffer.trim().endsWith(';')) {
    const rawSql = statementBuffer.trim();
    statementBuffer = ''; 
    rl.setPrompt('arbordb> ');

    const cleanSql = rawSql.replace(/;+$/, '').trim();
    if (cleanSql.toLowerCase() === 'show tables') {
      handleMetaCommand('.tables');
    } else if (cleanSql.toLowerCase().startsWith('describe ')) {
      const tName = cleanSql.split(' ')[1];
      handleMetaCommand(`.schema ${tName}`);
    } else if (cleanSql.toLowerCase().startsWith('desc ')) {
      const tName = cleanSql.split(' ')[1];
      handleMetaCommand(`.schema ${tName}`);
    } else {
      await executeSQL(rawSql);
    }
  } else {
    // Multi-line continuation
    rl.setPrompt('      -> ');
  }
  rl.prompt();
}).on('close', () => {
  saveHistory();
  console.log('\nBye');
  process.exit(0);
});

function handleMetaCommand(cmd) {
  // Strip trailing spaces and semicolons to allow ".tables ;" to work securely
  const cleanCmd = cmd.replace(/[\s;]+$/, '');
  
  switch (cleanCmd) {
    case '.exit':
    case '.quit':
      saveHistory();
      console.log('Bye');
      process.exit(0);
      break;
    case '.clear':
      console.clear();
      break;
    case '.tables': {
      const tables = engine.listTables();
      printTable(tables, ['name', 'primaryKey', 'rowCount']);
      break;
    }
    case '.help':
      console.log(chalk.bold('List of all monitor commands:'));
      console.log('  .exit, .quit    Exit the monitor');
      console.log('  .tables         List all tables');
      console.log('  .schema <table> Describe table schema');
      console.log('  \\c              Clear the current input statement');
      console.log('  .clear          Clear screen');
      console.log('  .help           Print this menu');
      break;
    default:
      if (cleanCmd.startsWith('.schema ')) {
        const tName = cleanCmd.split(' ')[1];
        const info = engine.getTableInfo(tName);
        if (!info) {
          console.log(chalk.red(`ERROR: Table '${tName}' does not exist.`));
        } else {
          const schemaRows = Object.entries(info.schema).map(([name, type]) => ({ Column: name, Type: type }));
          printTable(schemaRows);
        }
      } else {
        console.log(`Unknown command: ${cleanCmd}. Type .help for help.`);
      }
  }
}

async function executeSQL(sql) {
  rl.setPrompt('arbordb> ');
  const start = Date.now();

  try {
    const schemaMap = engine.getSchemaMap();
    const queryResult = processQuery(sql, schemaMap);
    
    // Check if it's an unsupported operation
    if (queryResult.ast.type === 'SELECT' && queryResult.ast.columns[0] === '*' && queryResult.ast.columns.length === 1 && Object.keys(schemaMap).length === 0 && !schemaMap[queryResult.ast.table]) {
        // Table doesn't exist, executor will catch but good to warn
    }

    const { ast, command, optimized } = queryResult;
    const engineResponse = await engine.callEngine(command);

    const ms = (Date.now() - start) / 1000;

    if (ast.type === 'SELECT') {
      const rows = engineResponse.rows || [];
      printTable(rows);
      console.log(chalk.grey(`${rows.length} rows in set (${ms.toFixed(3)} sec)`));
    } else {
      const affectedRows = engineResponse.affected_rows || 0;
      console.log(chalk.green(`Query OK, ${affectedRows} rows affected (${ms.toFixed(3)} sec)`));
    }

    if (optimized && optimized.optimizationHint && process.env.DEBUG === 'true') {
      console.log(chalk.yellow(`Optimizer Strategy: ${optimized.optimizationHint.strategy}`));
    }

  } catch (err) {
    console.log(chalk.red(`ERROR: ${err.message}`));
  }
}
