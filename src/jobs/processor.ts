import axios, { AxiosError } from 'axios';
import { Job } from 'bullmq';
import { MongoClient, Db, Collection, ObjectId } from 'mongodb';
import config from '../config';
import logger from '../utils/logger';

// --- Reusable Clients ---
let mongoClient: MongoClient;
let db: Db;
let businessCollection: Collection;

// --- Utility Functions ---
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

/**
 * Connects to MongoDB and initializes collection objects.
 */
export async function connectToMongo() {
  if (mongoClient) return;

  try {
    mongoClient = new MongoClient(config.mongo.uri);
    await mongoClient.connect();
    db = mongoClient.db(config.mongo.dbName);
    businessCollection = db.collection(config.mongo.collectionName);
    // The linking collection will be handled by the worker now

    logger.info('Successfully connected to MongoDB.');
    logger.info('Assuming MongoDB indexes are already in place.');

  } catch (error) {
    logger.error('Failed to connect to MongoDB:', error);
    process.exit(1);
  }
}

/**
 * The main processing function for a single Bubble.io _id.
 * This function now returns the documents to be inserted, rather than inserting them itself.
 */
export default async function processBubbleData(job: Job) {
  const { bubbleId } = job.data;

  try {
    const apiResponse = (await axios.get(`${config.bubble.baseUrl}/${bubbleId}`, {
      headers: { 'Authorization': `Bearer ${config.bubble.apiToken}` },
    })).data;

    const bubbleRecord = apiResponse?.response?.results?.[0];

    if (!bubbleRecord) {
        throw new Error(`Could not find record in Bubble API response for bubbleId: ${bubbleId}`);
    }

    // This array will hold the link documents to be created
    const linksToCreate = [];

    // --- STRATEGY 1: Direct ID Match ---
    const uniqueIdFromBubble = bubbleRecord.Unique_ID_1 || bubbleRecord.Unique_ID;

    if (uniqueIdFromBubble) {
        const directQuery = { old_pin_code: uniqueIdFromBubble };
        const matchingDocs = await businessCollection.find(directQuery).toArray();

        if (matchingDocs.length > 0) {
            for (const doc of matchingDocs) {
                linksToCreate.push({
                    bubble_id: bubbleId,
                    mongo_business_id: doc._id,
                    mongo_business_name: doc.name,
                    match_type: 'direct_id',
                    linked_on: new Date(),
                });
            }
            return { status: 'success', match_type: 'direct_id', links: linksToCreate };
        }
    }

    // Bubble Fields
    const site = bubbleRecord.site_1;
    const name = bubbleRecord.name_1;
    const phone = bubbleRecord.phone_1;
    const email = bubbleRecord.email_1;
    const address = bubbleRecord.full_address_1;

    // --- STRATEGY 2: Fallback using Website (High Confidence) ---
    if (site) {
        const orConditions = [];
        if (name) orConditions.push({ name: { $regex: new RegExp(normalizeString(name), 'i') } });
        if (phone) {
            const digits = phone.replace(/\D/g, '');
            if (digits.length > 6) orConditions.push({ "contact.phone": { $regex: digits } });
        }
        if (email) orConditions.push({ "contact.email": { $regex: new RegExp(normalizeString(email), 'i') } });
        if (address) orConditions.push({ "address.full_address": { $regex: new RegExp(normalizeString(address), 'i') } });

        if (orConditions.length > 0) {
            const query = { $and: [{ website: { $regex: new RegExp(normalizeUrl(site), 'i') } }, { $or: orConditions }]};
            const matches = await businessCollection.find(query).toArray();
            if (matches.length > 0) {
                 for (const doc of matches) {
                    linksToCreate.push({
                        bubble_id: bubbleId,
                        mongo_business_id: doc._id,
                        mongo_business_name: doc.name,
                        match_type: 'fallback_site_plus_field',
                        linked_on: new Date(),
                    });
                }
                return { status: 'success', match_type: 'fallback_with_site', links: linksToCreate };
            }
        }
    }

    // --- STRATEGY 3: Fallback without Website (Medium Confidence) ---
    if (name && (address || phone)) {
        const orConditions = [];
        if (address) orConditions.push({ "address.full_address": { $regex: new RegExp(normalizeString(address), 'i') } });
        if (phone) {
            const digits = phone.replace(/\D/g, '');
            if (digits.length > 6) orConditions.push({ "contact.phone": { $regex: digits } });
        }

        if(orConditions.length > 0) {
            const query = { $and: [{ name: { $regex: new RegExp(normalizeString(name), 'i') } }, { $or: orConditions }]};
            const matches = await businessCollection.find(query).toArray();
            if (matches.length > 0) {
                for (const doc of matches) {
                    linksToCreate.push({
                        bubble_id: bubbleId,
                        mongo_business_id: doc._id,
                        mongo_business_name: doc.name,
                        match_type: 'fallback_no_site',
                        linked_on: new Date(),
                    });
                }
                return { status: 'success', match_type: 'fallback_no_site', links: linksToCreate };
            }
        }
    }

    // --- If all strategies fail ---
    return { status: 'unmatched', bubbleId: bubbleId };

  } catch (error) {
    if (axios.isAxiosError(error)) {
        const axiosError = error as AxiosError;
        // Log less severe errors without the full stack trace for cleaner logs
        logger.error(`[Job ${job.id}] Axios error for Bubble ID ${bubbleId}: ${axiosError.message}`);
        if (axiosError.response?.status === 429) throw new Error('Rate limited by Bubble API');
    } else {
        logger.error(`[Job ${job.id}] Unexpected error for Bubble ID ${bubbleId}:`, error);
    }
    // Re-throw the error to make the job fail and be retried
    throw error;
  }
}
