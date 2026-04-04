'use strict';

// File parser — handles CSV (via papaparse) and Excel (via xlsx) uploads,
// normalizing them into a common { headers, rows } format.

const Papa = require('papaparse');
const XLSX = require('xlsx');
const { UploadError } = require('../errors');

function parseCSV(buffer, options = {}) {
  try {
    const csvString = buffer.toString('utf-8');

    const result = Papa.parse(csvString, {
      header: options.header !== false,
      delimiter: options.delimiter || undefined,
      skipEmptyLines: true,
      dynamicTyping: true,
      transformHeader: (header) => header.trim().toLowerCase(),
    });

    if (result.errors.length > 0) {
      const critical = result.errors.filter(e => e.type === 'Quotes' || e.type === 'FieldMismatch');
      if (critical.length > 0) {
        throw new UploadError('CSV parsing failed', {
          errors: critical.map(e => ({
            type: e.type, code: e.code, message: e.message, row: e.row,
          })),
        });
      }
    }

    if (!result.data || result.data.length === 0) {
      throw new UploadError('CSV file is empty or contains no valid data');
    }

    const headers = result.meta.fields || Object.keys(result.data[0]);
    return { headers, rows: result.data, totalRows: result.data.length };
  } catch (err) {
    if (err instanceof UploadError) throw err;
    throw new UploadError(`Failed to parse CSV: ${err.message}`);
  }
}

function parseExcel(buffer, options = {}) {
  try {
    const workbook = XLSX.read(buffer, { type: 'buffer' });

    if (!workbook.SheetNames || workbook.SheetNames.length === 0) {
      throw new UploadError('Excel file contains no sheets');
    }

    const sheetName = options.sheetName || workbook.SheetNames[0];
    const sheet = workbook.Sheets[sheetName];

    if (!sheet) {
      throw new UploadError(`Sheet '${sheetName}' not found`, {
        availableSheets: workbook.SheetNames,
      });
    }

    const jsonData = XLSX.utils.sheet_to_json(sheet, { defval: null, raw: true });

    if (!jsonData || jsonData.length === 0) {
      throw new UploadError('Excel sheet is empty or contains no valid data');
    }

    const headers = Object.keys(jsonData[0]).map(h => h.trim().toLowerCase());
    const rows = jsonData.map(row => {
      const normalized = {};
      for (const [key, value] of Object.entries(row)) {
        normalized[key.trim().toLowerCase()] = value;
      }
      return normalized;
    });

    return { headers, rows, totalRows: rows.length, sheetName };
  } catch (err) {
    if (err instanceof UploadError) throw err;
    throw new UploadError(`Failed to parse Excel file: ${err.message}`);
  }
}

function parseFile(buffer, filename, options = {}) {
  const ext = filename.toLowerCase().split('.').pop();

  switch (ext) {
    case 'csv':
      return { ...parseCSV(buffer, options), format: 'csv' };
    case 'xlsx': case 'xls':
      return { ...parseExcel(buffer, options), format: 'xlsx' };
    default:
      throw new UploadError(`Unsupported file format: .${ext}`, {
        supportedFormats: ['csv', 'xlsx', 'xls'],
      });
  }
}

module.exports = { parseCSV, parseExcel, parseFile };
