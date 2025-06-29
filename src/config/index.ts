import dotenv from 'dotenv';
import path from 'path';

// Load .env file from the root directory
dotenv.config({ path: path.resolve(__dirname, '../../.env') });

const config = {
  bubble: {
    apiToken: process.env.BUBBLE_API_TOKEN,
    baseUrl: process.env.BUBBLE_API_BASE_URL,
  },
  mongo: {
    uri: process.env.MONGO_URI || 'mongodb://localhost:27017',
    dbName: process.env.MONGO_DB_NAME || 'Archer_Group',
    collectionName: process.env.MONGO_COLLECTION_NAME || 'business',
  },
  redis: {
    host: process.env.REDIS_HOST || 'localhost',
    port: parseInt(process.env.REDIS_PORT || '6379', 10),
  },
  logLevel: process.env.LOG_LEVEL || 'info',
  concurrentJobs: parseInt(process.env.CONCURRENT_JOBS || '5', 10),
  bubbleIdsFilePath: process.env.BUBBLE_IDS_FILE_PATH || './bubble_ids.csv',
  // NEW: Add a configurable limit for the producer
  producerJobLimit: parseInt(process.env.PRODUCER_JOB_LIMIT || '0', 10) || Infinity,
};

// Basic validation
if (!config.bubble.apiToken || !config.bubble.baseUrl) {
  console.error("FATAL ERROR: Bubble API token or Base URL is not configured in .env file.");
  process.exit(1);
}

export default config;
