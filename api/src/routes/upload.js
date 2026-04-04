'use strict';

// POST /upload — handles CSV and Excel file uploads with schema validation
// and batch insertion into a target table.

const express = require('express');
const multer = require('multer');
const path = require('path');
const Joi = require('joi');
const { parseFile } = require('../services/fileParser');
const { validateSchema, validateBatch } = require('../services/validator');
const { batchInsert } = require('../services/batchInsert');
const engine = require('../engine');
const metricsService = require('../services/metrics');
const { UploadError, ValidationError, NotFoundError } = require('../errors');

const router = express.Router();

const maxFileSize = parseInt(process.env.MAX_FILE_SIZE_MB || '50', 10) * 1024 * 1024;

const upload = multer({
  storage: multer.memoryStorage(),
  limits: { fileSize: maxFileSize, files: 1 },
  fileFilter: (_req, file, cb) => {
    const ext = path.extname(file.originalname).toLowerCase();
    if (['.csv', '.xlsx', '.xls'].includes(ext)) {
      cb(null, true);
    } else {
      cb(new UploadError(`Unsupported file type: ${ext}`, { supportedFormats: ['csv', 'xlsx', 'xls'] }));
    }
  },
});

const uploadParamsSchema = Joi.object({
  table: Joi.string().trim().min(1).max(128).required()
    .messages({ 'any.required': 'Target table name is required' }),
  batchSize: Joi.number().integer().min(1).max(10000).default(100),
  stopOnError: Joi.boolean().default(false),
});

router.post('/', upload.single('file'), async (req, res, next) => {
  const totalStart = Date.now();

  try {
    const { error: paramsError, value: params } = uploadParamsSchema.validate(req.body);
    if (paramsError) {
      throw new ValidationError('Invalid upload parameters', {
        details: paramsError.details.map(d => d.message),
      });
    }

    if (!req.file) {
      throw new UploadError('No file provided. Please upload a CSV or Excel file.');
    }

    const { table, batchSize, stopOnError } = params;

    const tableInfo = engine.getTableInfo(table);
    if (!tableInfo) {
      throw new NotFoundError(`Table '${table}' does not exist. Create the table first.`);
    }

    // Parse file
    const parseStart = Date.now();
    const fileData = parseFile(req.file.buffer, req.file.originalname);
    const parseTimeMs = Date.now() - parseStart;

    // Validate columns against schema
    const schemaValidation = validateSchema(fileData.headers, tableInfo.schema);
    if (!schemaValidation.valid) {
      throw new ValidationError('File columns do not match table schema', {
        missingColumns: schemaValidation.missingColumns,
        extraColumns: schemaValidation.extraColumns,
        expectedColumns: Object.keys(tableInfo.schema),
        fileColumns: fileData.headers,
      });
    }

    // Validate rows
    const validationStart = Date.now();
    const validationResult = validateBatch(fileData.rows, tableInfo.schema, {
      primaryKey: tableInfo.primaryKey, coerce: true,
    });
    const validationTimeMs = Date.now() - validationStart;

    // Insert valid rows
    const insertStart = Date.now();
    let insertionResult = { insertedCount: 0, failedCount: 0, errors: [] };
    if (validationResult.validRows.length > 0) {
      insertionResult = await batchInsert(table, validationResult.validRows, tableInfo.schema, {
        primaryKey: tableInfo.primaryKey, batchSize, stopOnError,
      });
    }
    const insertTimeMs = Date.now() - insertStart;
    const totalTimeMs = Date.now() - totalStart;

    metricsService.recordUpload({
      filename: req.file.originalname, format: fileData.format, table,
      rowsInserted: insertionResult.insertedCount,
      rowsFailed: insertionResult.failedCount + validationResult.invalidCount,
      executionTimeMs: totalTimeMs,
    });

    res.json({
      status: 'ok',
      upload: {
        filename: req.file.originalname, format: fileData.format,
        fileSize: req.file.size, totalRows: fileData.totalRows,
      },
      validation: {
        validCount: validationResult.validCount,
        invalidCount: validationResult.invalidCount,
        duplicateKeys: validationResult.duplicateKeys.length,
        errors: validationResult.errors.slice(0, 50),
      },
      insertion: {
        insertedCount: insertionResult.insertedCount,
        failedCount: insertionResult.failedCount,
        errors: insertionResult.errors.slice(0, 50),
      },
      metrics: { parseTimeMs, validationTimeMs, insertTimeMs, totalTimeMs },
    });
  } catch (err) {
    next(err);
  }
});

module.exports = router;
