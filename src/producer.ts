import { Queue } from 'bullmq';
import fs from 'fs';
import csv from 'csv-parser';
import config from './config';
import { redisConnection } from './queues/connection';
import logger from './utils/logger';

const QUEUE_NAME = 'bubble-processing-queue';

async function enqueueJobs() {
  return new Promise<void>((resolve, reject) => {
    // NEW: Get the job limit from the config
    const jobLimit = config.producerJobLimit;
    logger.info('--- Starting Producer ---');
    if (jobLimit !== Infinity) {
        logger.info(`Job limit is set to: ${jobLimit}`);
    }

    const filePath = config.bubbleIdsFilePath;
    if (!fs.existsSync(filePath)) {
      const errorMsg = `FATAL: ID file not found at path: ${filePath}`;
      logger.error(errorMsg);
      return reject(new Error(errorMsg));
    }

    const myQueue = new Queue(QUEUE_NAME, { connection: redisConnection });
    const jobPromises: Promise<any>[] = [];

    const fileStream = fs.createReadStream(filePath);

    fileStream
      .pipe(csv({ headers: false }))
      .on('data', (row) => {
        // NEW: Check if the job limit has been reached
        if (jobPromises.length >= jobLimit) {
            // Stop reading the file if the limit is hit
            fileStream.destroy();
            return;
        }

        const bubbleId = row[0]?.trim();
        if (bubbleId && !bubbleId.startsWith('#')) {
            const jobPromise = myQueue.add('process-bubble-id', { bubbleId }, {
                attempts: 3,
                backoff: {
                    type: 'exponential',
                    delay: 5000,
                }
            });
            jobPromises.push(jobPromise);
        }
      })
      .on('end', async () => {
        try {
            await Promise.all(jobPromises);
            const enqueuedCount = jobPromises.length;

            logger.info(`--- Producer Finished ---`);
            logger.info(`Total jobs enqueued: ${enqueuedCount}`);
            await myQueue.close();
            resolve();
        } catch (err) {
            logger.error('An error occurred during job enqueuing:', err);
            reject(err);
        }
      })
      .on('error', (err) => {
        // Ignore the error that occurs when we manually destroy the stream
        if (err.message.includes('Stream was destroyed')) return;
        logger.error('Error processing CSV file:', err);
        reject(err);
      });
  });
}

enqueueJobs()
  .catch(err => {
    logger.error('Producer failed:', err);
    process.exit(1);
  })
  .finally(() => {
    setTimeout(() => process.exit(0), 500);
  });
