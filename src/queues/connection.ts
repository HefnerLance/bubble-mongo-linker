import { ConnectionOptions } from 'bullmq';
import config from '../config';

// Reusable Redis connection options for BullMQ
export const redisConnection: ConnectionOptions = {
  host: config.redis.host,
  port: config.redis.port,
};
