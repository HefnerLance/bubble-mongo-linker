// src/producer.ts

import { Queue } from 'bullmq';
import axios from 'axios';
import config from './config';
import { redisConnection } from './queues/connection';
import logger from './utils/logger';

const QUEUE_NAME = 'bubble-processing-queue';
const BATCH_SIZE = 100; // How many records to fetch from Bubble in each API call
const CURSOR_LIMIT = 49900; // Set a safe limit below Bubble's 50k max

async function enqueueJobsFromAPI() {
  logger.info('--- Starting Producer (Advanced API Mode) ---');

  const myQueue = new Queue(QUEUE_NAME, { connection: redisConnection });
  let totalEnqueued = 0;
  let keepFetching = true;

  // We'll sort by creation date and use this to paginate beyond the cursor limit
  let lastProcessedDate: string | null = null;

  while (keepFetching) {
    let cursor = 0;
    let inPageLoop = true;
    logger.info(`Starting a new pagination loop. Records created after: ${lastProcessedDate || 'the beginning'}`);

    while (inPageLoop) {
      try {
        // --- Build the API Query ---
        const params: any = {
          limit: BATCH_SIZE,
          cursor: cursor,
          sort_field: 'Created Date', // Sort by the creation date
          sort_descending: 'false'   // In ascending order (oldest first)
        };

        // If we have a lastProcessedDate, use it to constrain the search
        if (lastProcessedDate) {
          params.constraints = JSON.stringify([
            { key: 'Created Date', constraint_type: 'greater than', value: lastProcessedDate }
          ]);
        }

        const response = await axios.get(`${config.bubble.baseUrl}`, {
          headers: { 'Authorization': `Bearer ${config.bubble.apiToken}` },
          params: params
        });

        const results = response.data?.response?.results;

        if (!results || results.length === 0) {
          // If we get no results, we're done with this loop (and likely all data)
          inPageLoop = false;
          keepFetching = false;
          continue;
        }

        const jobPromises = results.map((record: any) => {
          const bubbleId = record._id;
          if (bubbleId) {
            return myQueue.add('process-bubble-id', { bubbleId }, {
              attempts: 3,
              backoff: { type: 'exponential', delay: 5000 },
            });
          }
          return Promise.resolve();
        });

        await Promise.all(jobPromises);
        totalEnqueued += results.length;

        // --- Update Cursor and Last Processed Date ---
        cursor += results.length;
        // The last record in this batch has the latest date so far
        const lastRecord = results[results.length - 1];
        lastProcessedDate = lastRecord['Created Date'];

        logger.info(`Enqueued ${results.length} jobs. Cursor at: ${cursor}. Last date: ${lastProcessedDate}`);

        // If we are approaching the cursor limit, break out of this inner loop
        // The outer loop will then restart the process with an updated date filter
        if (cursor >= CURSOR_LIMIT) {
          logger.warn(`Cursor limit of ${CURSOR_LIMIT} reached. Resetting cursor with a new date filter.`);
          inPageLoop = false;
        }

        // If the API returns fewer records than our batch size, it means we've reached the end of the current query
        if (results.length < BATCH_SIZE) {
            inPageLoop = false;
            keepFetching = false;
        }

      } catch (error: any) {
        logger.error('Error fetching data from Bubble API:', error.message);
        // Stop all fetching on a critical error
        inPageLoop = false;
        keepFetching = false;
      }
    }
  }

  logger.info(`--- Producer Finished ---`);
  logger.info(`Total jobs enqueued: ${totalEnqueued}`);
  await myQueue.close();
}

enqueueJobsFromAPI()
  .catch(err => {
    logger.error('Producer failed:', err);
    process.exit(1);
  })
  .finally(() => {
    // Add a short delay to allow logs to flush before exiting
    setTimeout(() => process.exit(0), 1000);
  });
