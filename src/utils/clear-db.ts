// src/utils/clear-db.ts

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
        maxRetriesPerRequest: null
    });
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

  // Clear MongoDB Collections
  let mongoClient;
  try {
    mongoClient = new MongoClient(config.mongo.uri);
    await mongoClient.connect();
    const db = mongoClient.db(config.mongo.dbName);

    // Clear the old linking collection (if it exists)
    const linkingCollection = db.collection('linking_collection');
    const deleteLinkingResult = await linkingCollection.deleteMany({});
    logger.info(`Cleared ${deleteLinkingResult.deletedCount} documents from MongoDB linking_collection.`);

    // NEW: Clear the authoritative collection
    const authoritativeCollection = db.collection(config.mongo.authoritativeCollectionName);
    const deleteAuthoritativeResult = await authoritativeCollection.deleteMany({});
    logger.info(`Cleared ${deleteAuthoritativeResult.deletedCount} documents from MongoDB ${config.mongo.authoritativeCollectionName}.`);

  } catch (error) {
    logger.error('Failed to clear MongoDB collections:', error);
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
