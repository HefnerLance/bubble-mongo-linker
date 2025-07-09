// src/workers/bubbleWorker.ts

import { Worker, Job } from 'bullmq';
import config from '../config';
import { redisConnection } from '../queues/connection';
import logger from '../utils/logger';
import issueLogger from '../utils/issue-logger';
import processBubbleData, { connectToMongo } from '../jobs/processor';

const QUEUE_NAME = 'bubble-processing-queue';

const sessionStats = {
  startTime: new Date(),
  jobsProcessed: 0,
  newRecordsCreated: 0,
  duplicatesFound: 0,
  directMatches: 0,
  fallbackMatches: 0,
  unmatched: 0,
  skipped: 0,
  failed: 0,
  notFound: 0, // Add a counter for not_found status
};

async function startWorker() {
  await connectToMongo();

  logger.info(`Starting worker for queue: ${QUEUE_NAME}`);
  logger.info(`Concurrency: ${config.concurrentJobs}`);

  const worker = new Worker(QUEUE_NAME, processBubbleData, {
    connection: redisConnection,
    concurrency: config.concurrentJobs,
  });

  worker.on('completed', (job: Job, result: any) => {
    sessionStats.jobsProcessed++;

    // FIXED: Add a log line for every completed job for real-time feedback.
    logger.info(`Job ${job.id} completed. Status: ${result.status}, Match Type: ${result.match_type || 'N/A'}`);

    if (result.status === 'success') {
        if (result.match_type === 'duplicate') {
            sessionStats.duplicatesFound++;
        } else {
            sessionStats.newRecordsCreated++;
            if (result.match_type === 'direct_id') sessionStats.directMatches++;
            if (result.match_type === 'fallback_match') sessionStats.fallbackMatches++;
            if (result.match_type === 'unmatched') sessionStats.unmatched++;
        }
    } else if (result.status === 'skipped') {
        sessionStats.skipped++;
        issueLogger.warn('Skipped Record', { bubbleId: job.data.bubbleId, reason: result.reason, jobId: job.id });
    } else if (result.status === 'not_found') {
        sessionStats.notFound++;
        // The issueLogger already logs this in the processor, so no need to log again here.
    }
  });

  worker.on('failed', (job: Job | undefined, err: Error) => {
    sessionStats.jobsProcessed++;
    sessionStats.failed++;
    if (job) {
      issueLogger.error('Failed Job', { bubbleId: job.data.bubbleId, jobId: job.id, error: err.message });
      logger.error(`Job ${job.id} failed: ${err.message}`);
    } else {
      issueLogger.error('A job failed with an unknown ID.', { error: err.message });
    }
  });

  const shutdown = async () => {
    logger.info('--- Shutting down worker... ---');
    await worker.close();

    const endTime = new Date();
    const durationMs = endTime.getTime() - sessionStats.startTime.getTime();
    const durationSec = (durationMs / 1000).toFixed(2);
    const jobsPerSecond = (sessionStats.jobsProcessed / (durationMs / 1000)).toFixed(2);

    console.log("\n--- Worker Session Report ---");
    console.log(`- Duration:              ${durationSec}s (~${jobsPerSecond} jobs/sec)`);
    console.log(`- Jobs Processed:        ${sessionStats.jobsProcessed}`);
    console.log("---------------------------------");
    console.log(`- New Records Created:   ${sessionStats.newRecordsCreated}`);
    console.log(`- Duplicates Found:      ${sessionStats.duplicatesFound}`);
    console.log("---------------------------------");
    console.log(`- Matches (Direct ID):   ${sessionStats.directMatches}`);
    console.log(`- Matches (Fallback):    ${sessionStats.fallbackMatches}`);
    console.log(`- Unmatched w/ Mongo:    ${sessionStats.unmatched}`);
    console.log(`- Skipped (no key):      ${sessionStats.skipped}`);
    console.log(`- Not Found (in API):    ${sessionStats.notFound}`);
    console.log(`- Failed:                ${sessionStats.failed}`);
    console.log("---------------------------------\n");

    process.exit(0);
  };

  process.on('SIGINT', shutdown);
}

startWorker().catch(err => logger.error('Failed to start worker:', err));

