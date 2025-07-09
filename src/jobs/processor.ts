// src/jobs/processor.ts

import axios, { AxiosError } from 'axios';
import { Job } from 'bullmq';
import { MongoClient, Db, Collection, ObjectId } from 'mongodb';
import config from '../config';
import logger from '../utils/logger';
import issueLogger from '../utils/issue-logger';

let mongoClient: MongoClient;
let db: Db;
let businessCollection: Collection;
let authoritativeCollection: Collection;

function normalizeString(str: string | undefined): string {
    if (!str) return '';
    return str.toLowerCase().replace(/[\s.,\/#!$%\^&\*;:{}=\-_`~()']/g, "").trim();
}

function normalizeUrl(url: string | undefined): string {
    if (!url) return '';
    try {
        const urlObj = new URL(url);
        return urlObj.hostname.replace(/^www\./, '');
    } catch {
        return url.toLowerCase().replace(/^(https?:\/\/)?(www\.)?/, '').split('/')[0];
    }
}

export async function connectToMongo() {
  if (mongoClient) return;
  try {
    mongoClient = new MongoClient(config.mongo.uri);
    await mongoClient.connect();
    db = mongoClient.db(config.mongo.dbName);
    businessCollection = db.collection(config.mongo.collectionName);
    authoritativeCollection = db.collection(config.mongo.authoritativeCollectionName);

    logger.info('Successfully connected to MongoDB.');
    await authoritativeCollection.createIndex({ website_normalized: 1, address_normalized: 1 }, { unique: true });
    logger.info('Ensured indexes are in place for authoritative collection.');

  } catch (error) {
    logger.error('Failed to connect to MongoDB:', error);
    process.exit(1);
  }
}

export default async function processBubbleData(job: Job) {
  const { bubbleId } = job.data;

  try {
    const apiResponse = (await axios.get(`${config.bubble.baseUrl}/${bubbleId}`, {
      headers: { 'Authorization': `Bearer ${config.bubble.apiToken}` },
    })).data;

    // FIXED: Access the response object directly. When fetching a single object by ID,
    // Bubble.io returns the object itself, not an array of results.
    const bubbleRecord = apiResponse?.response;

    if (!bubbleRecord) {
        const reason = `Record not found or accessible via API for bubbleId: ${bubbleId}`;
        issueLogger.warn('Record Not Found', { bubbleId, reason, jobId: job.id });
        return { status: 'not_found', reason, bubbleId };
    }

    const site = bubbleRecord.site_1;
    const address = bubbleRecord.full_address_1;

    const deDuplicationKey = {
        website_normalized: normalizeUrl(site),
        address_normalized: normalizeString(address),
    };

    if (!deDuplicationKey.website_normalized && !deDuplicationKey.address_normalized) {
        return { status: 'skipped', reason: 'Missing website and address for de-duplication', bubbleId };
    }

    const existingAuthoritativeDoc = await authoritativeCollection.findOne(deDuplicationKey);

    if (existingAuthoritativeDoc) {
        await authoritativeCollection.updateOne(
            { _id: existingAuthoritativeDoc._id },
            { $addToSet: { bubble_ids: bubbleId } }
        );
        return { status: 'success', match_type: 'duplicate', bubbleId };
    }

    const name = bubbleRecord.name_1;
    const phone = bubbleRecord.phone_1;
    const email = bubbleRecord.email_1;

    let mongoMatch: { mongo_business_id: ObjectId | null; match_type: string; } = { mongo_business_id: null, match_type: 'unmatched' };

    const uniqueIdFromBubble = bubbleRecord.Unique_ID_1 || bubbleRecord.Unique_ID;
    if (uniqueIdFromBubble) {
        const matchingDoc = await businessCollection.findOne({ old_pin_code: uniqueIdFromBubble });
        if (matchingDoc) {
            mongoMatch = { mongo_business_id: matchingDoc._id, match_type: 'direct_id' };
        }
    }

    if (!mongoMatch.mongo_business_id) {
        const queryConditions: any[] = [];
        if (name) queryConditions.push({ name: { $regex: new RegExp(normalizeString(name), 'i') } });
        if (phone) {
            const digits = phone.replace(/\D/g, '');
            if (digits.length > 6) queryConditions.push({ "contact.phone": { $regex: digits } });
        }

        if (queryConditions.length > 0) {
            const query = { $and: [{ website: { $regex: new RegExp(normalizeUrl(site), 'i') } }, { $or: queryConditions }]};
            const matches = await businessCollection.find(query).toArray();
            if (matches.length === 1) {
                mongoMatch = { mongo_business_id: matches[0]._id, match_type: 'fallback_match' };
            }
        }
    }

    const newAuthoritativeDoc = {
        ...deDuplicationKey,
        name: name,
        website: site,
        address: address,
        phone: phone,
        email: email,
        bubble_ids: [bubbleId],
        ...mongoMatch,
        created_at: new Date(),
        updated_at: new Date(),
    };

    await authoritativeCollection.insertOne(newAuthoritativeDoc);

    return { status: 'success', match_type: newAuthoritativeDoc.match_type, bubbleId };

  } catch (error) {
    if (axios.isAxiosError(error)) {
        const axiosError = error as AxiosError;
        // Don't flood the logs if it's a 404 Not Found error, as this is now expected for some records.
        if (axiosError.response?.status !== 404) {
             logger.error(`[Job ${job.id}] Axios error for Bubble ID ${bubbleId}: ${axiosError.message}`);
        }
    } else {
        logger.error(`[Job ${job.id}] Unexpected error for Bubble ID ${bubbleId}:`, error);
    }
    // Still throw the error to allow BullMQ to retry if it's a temporary network issue, etc.
    throw error;
  }
}
