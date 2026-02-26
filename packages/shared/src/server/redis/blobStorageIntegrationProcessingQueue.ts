import { Queue } from "bullmq";
import { QueueName } from "../queues";
import {
  createNewRedisInstance,
  redisQueueRetryOptions,
  getQueuePrefix,
} from "./redis";
import { logger } from "../logger";

export class BlobStorageIntegrationProcessingQueue {
  private static instance: Queue | null = null;

  public static getInstance(): Queue | null {
    if (BlobStorageIntegrationProcessingQueue.instance) {
      return BlobStorageIntegrationProcessingQueue.instance;
    }

    const newRedis = createNewRedisInstance({
      enableOfflineQueue: false,
      ...redisQueueRetryOptions,
    });

    BlobStorageIntegrationProcessingQueue.instance = newRedis
      ? new Queue(QueueName.BlobStorageIntegrationProcessingQueue, {
          connection: newRedis,
          prefix: getQueuePrefix(
            QueueName.BlobStorageIntegrationProcessingQueue,
          ),
          defaultJobOptions: {
            removeOnComplete: true,
            removeOnFail: true,
            attempts: 5,
            backoff: {
              type: "exponential",
              delay: 30000, // 30s initial backoff — prevents retry death loop on ClickHouse memory kills
            },
          },
        })
      : null;

    BlobStorageIntegrationProcessingQueue.instance?.on("error", (err) => {
      logger.error("BlobStorageIntegrationProcessingQueue error", err);
    });

    return BlobStorageIntegrationProcessingQueue.instance;
  }
}
