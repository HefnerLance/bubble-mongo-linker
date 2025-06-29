import { MongoClient } from 'mongodb';
import Redis from 'ioredis';
import config from '../config';
import logger from './logger';

const QUEUE_NAME = 'bubble-processing-queue';

async function clearDatabases() {
  logger.info('--- Clearing Databases for a Fresh Start ---');

  // Clear Redis Queue
  try {
    const redis = new Redis({
        host: config.redis.host,
        port: config.redis.port,
        maxRetriesPerRequest: null // Required for some versions
    });
    // This deletes all keys related to the queue, which is safer than FLUSHDB
    const keys = await redis.keys(`bull:${QUEUE_NAME}:*`);
    if (keys.length > 0) {
      await redis.del(keys);
      logger.info(`Cleared ${keys.length} keys from Redis for queue: ${QUEUE_NAME}`);
    } else {
        logger.info(`No keys found in Redis for queue: ${QUEUE_NAME}. Nothing to clear.`);
    }
    await redis.quit();
  } catch (error) {
    logger.error('Failed to clear Redis database:', error);
    throw error;
  }

  // Clear MongoDB Linking Collection
  let mongoClient;
  try {
    mongoClient = new MongoClient(config.mongo.uri);
    await mongoClient.connect();
    const db = mongoClient.db(config.mongo.dbName);
    const linkingCollection = db.collection('linking_collection');
    const deleteResult = await linkingCollection.deleteMany({});
    logger.info(`Cleared ${deleteResult.deletedCount} documents from MongoDB linking_collection.`);
  } catch (error) {
    logger.error('Failed to clear MongoDB collection:', error);
    throw error;
  } finally {
    if (mongoClient) {
      await mongoClient.close();
    }
  }

  logger.info('--- Databases cleared successfully ---');
}

clearDatabases().catch(err => {
    logger.error('An error occurred during database cleanup.', err);
    process.exit(1);
});
