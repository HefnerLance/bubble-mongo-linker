import winston from 'winston';
import fs from 'fs';
import path from 'path';

// Create a logs directory if it doesn't exist
const logDir = 'logs';
if (!fs.existsSync(logDir)) {
  fs.mkdirSync(logDir);
}

const { combine, timestamp, printf, json } = winston.format;

/**
 * A dedicated logger to write processing issues (unmatched, skipped, failed)
 * to a structured log file for later review.
 */
const issueLogger = winston.createLogger({
  level: 'info',
  format: combine(
    timestamp(),
    json() // Log in a structured JSON format
  ),
  transports: [
    new winston.transports.File({
      filename: path.join(logDir, 'processing-issues.log'),
      level: 'warn', // Log warnings and errors
    }),
  ],
  exitOnError: false,
});

export default issueLogger;
