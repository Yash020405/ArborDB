'use strict';

const chalk = require('chalk');

/**
 * Basic formatting utility to print MySQL-style ASCII tables
 */
function printTable(rows, columns) {
  if (!rows || rows.length === 0) {
    console.log('Empty set');
    return;
  }

  const cols = columns || Object.keys(rows[0]);
  
  // Calculate max width for each column
  const colWidths = cols.map(col => {
    let max = String(col).length;
    rows.forEach(row => {
      const valStr = row[col] === null ? 'NULL' : String(row[col]);
      if (valStr.length > max) max = valStr.length;
    });
    return max + 2; // +2 for padding
  });

  // Build horizontal separator
  const separator = '+' + colWidths.map(w => '-'.repeat(w)).join('+') + '+';

  // Print Header
  console.log(separator);
  let headerStr = '|';
  cols.forEach((col, i) => {
    headerStr += ' ' + chalk.cyan(String(col).padEnd(colWidths[i] - 1)) + '|';
  });
  console.log(headerStr);
  console.log(separator);

  // Print Rows
  rows.forEach(row => {
    let rowStr = '|';
    cols.forEach((col, i) => {
      const val = row[col] === null ? chalk.grey('NULL') : String(row[col]);
      const rawValLen = row[col] === null ? 4 : String(row[col]).length;
      rowStr += ' ' + val + ' '.repeat(colWidths[i] - rawValLen - 1) + '|';
    });
    console.log(rowStr);
  });

  console.log(separator);
}

module.exports = { printTable };
