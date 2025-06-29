import { Worker, Job } from 'bullmq';
import { MongoClient, Db, Collection } from 'mongodb'; // Import Mongo types
import config from '../config';
import { redisConnection } from '../queues/connection';
import logger from '../utils/logger';
import issueLogger from '../utils/issue-logger';
import processBubbleData, { connectToMongo } from '../jobs/processor';

const QUEUE_NAME = 'bubble-processing-queue';

// --- NEW: Batching Configuration ---
const BATCH_SIZE = 500; // Insert documents in batches of 500
const BATCH_TIMEOUT = 5000; // Flush batch every 5 seconds, regardless of size
let linkDocumentBatch: any[] = [];
let linkingCollection: Collection;
let batchTimeout: NodeJS.Timeout;

const sessionStats = {
  startTime: new Date(),
  jobsProcessed: 0,
  linksCreated: 0,
  directMatches: 0,
  fallbackWithSite: 0,
  fallbackNoSite: 0,
  unmatched: 0,
  skipped: 0,
  failed: 0,
};

// --- NEW: Function to flush the batch to MongoDB ---
async function flushBatch() {
    if (linkDocumentBatch.length === 0) {
        return;
    }

    // Create a copy and clear the original batch immediately
    const batchToInsert = [...linkDocumentBatch];
    linkDocumentBatch = [];

    try {
        await linkingCollection.insertMany(batchToInsert, { ordered: false }); // 'ordered: false' continues on duplicate key errors
        const count = batchToInsert.length;
        sessionStats.linksCreated += count;
        logger.info(`Successfully inserted batch of ${count} link documents.`);
    } catch (error: any) {
        logger.error('Failed to insert batch.', {
            error: error.message,
            // BulkWriteError contains detailed info about which documents failed
            writeErrors: error.writeErrors?.map((e: any) => e.err.errmsg)
        });
        // Optionally, add failed documents to an issue log for reprocessing
    }
}

async function startWorker() {
  await connectToMongo();

  // Get a reference to the linking collection
  const db = new MongoClient(config.mongo.uri).db(config.mongo.dbName);
  linkingCollection = db.collection('linking_collection');

  logger.info(`Starting worker for queue: ${QUEUE_NAME}`);
  logger.info(`Concurrency: ${config.concurrentJobs} | Batch Size: ${BATCH_SIZE}`);

  // Start the periodic batch flush
  batchTimeout = setInterval(flushBatch, BATCH_TIMEOUT);

  const worker = new Worker(QUEUE_NAME, processBubbleData, {
    connection: redisConnection,
    concurrency: config.concurrentJobs,
  });

  worker.on('completed', (job: Job, result: any) => {
    sessionStats.jobsProcessed++;
    if (result.status === 'success') {
        // Add the returned link documents to the batch
        if (result.links && result.links.length > 0) {
            linkDocumentBatch.push(...result.links);
        }
        // Update stats
        if (result.match_type === 'direct_id') sessionStats.directMatches++;
        if (result.match_type === 'fallback_with_site') sessionStats.fallbackWithSite++;
        if (result.match_type === 'fallback_no_site') sessionStats.fallbackNoSite++;
    } else if (result.status === 'unmatched') {
        sessionStats.unmatched++;
        issueLogger.warn('Unmatched Record', { bubbleId: result.bubbleId, jobId: job.id });
    } else if (result.status === 'skipped') {
        sessionStats.skipped++;
        issueLogger.warn('Skipped Record', { bubbleId: job.data.bubbleId, reason: result.reason, jobId: job.id });
    }

    // Flush the batch if it's full
    if (linkDocumentBatch.length >= BATCH_SIZE) {
        flushBatch();
    }
  });

  worker.on('failed', (job: Job | undefined, err: Error) => {
    sessionStats.jobsProcessed++;
    sessionStats.failed++;
    if (job) {
      issueLogger.error('Failed Job', { bubbleId: job.data.bubbleId, jobId: job.id, error: err.message, stack: err.stack });
      logger.error(`Job ${job.id} failed: ${err.message}`);
    } else {
      issueLogger.error('A job failed with an unknown ID.', { error: err.message });
    }
  });

  const shutdown = async () => {
    logger.info('--- Shutting down worker... ---');
    clearInterval(batchTimeout); // Stop the periodic flush
    await worker.close();
    logger.info('Flushing final batch of documents...');
    await flushBatch(); // Flush any remaining documents

    const endTime = new Date();
    const durationMs = endTime.getTime() - sessionStats.startTime.getTime();
    const durationSec = (durationMs / 1000).toFixed(2);
    const jobsPerSecond = (sessionStats.jobsProcessed / (durationMs / 1000)).toFixed(2);

    console.log("\n--- Worker Session Report ---");
    console.log(`- Duration:        ${durationSec}s (~${jobsPerSecond} jobs/sec)`);
    console.log(`- Jobs Processed:  ${sessionStats.jobsProcessed}`);
    console.log(`- Links Created:   ${sessionStats.linksCreated}`);
    console.log("-------------------------------");
    console.log(`- Matches (Direct):  ${sessionStats.directMatches}`);
    console.log(`- Matches (Site):    ${sessionStats.fallbackWithSite}`);
    console.log(`- Matches (No Site): ${sessionStats.fallbackNoSite}`);
    console.log(`- Unmatched:         ${sessionStats.unmatched}`);
    console.log(`- Skipped:           ${sessionStats.skipped}`);
    console.log(`- Failed:            ${sessionStats.failed}`);
    console.log("-------------------------------\n");

    process.exit(0);
  };

  process.on('SIGINT', shutdown);
}

startWorker().catch(err => logger.error('Failed to start worker:', err));
