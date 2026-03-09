import { Queue } from "bullmq";
import { QueueName, TQueueJobTypes } from "../queues";
import {
  createNewRedisInstance,
  redisQueueRetryOptions,
  getQueuePrefix,
} from "./redis";
import { logger } from "../logger";

export class ClickhouseWriterDlqQueue {
  private static instance: Queue<
    TQueueJobTypes[QueueName.ClickhouseWriterDlqQueue]
  > | null = null;

  public static getInstance(): Queue<
    TQueueJobTypes[QueueName.ClickhouseWriterDlqQueue]
  > | null {
    if (ClickhouseWriterDlqQueue.instance) {
      return ClickhouseWriterDlqQueue.instance;
    }

    const newRedis = createNewRedisInstance({
      enableOfflineQueue: false,
      ...redisQueueRetryOptions,
    });

    ClickhouseWriterDlqQueue.instance = newRedis
      ? new Queue<TQueueJobTypes[QueueName.ClickhouseWriterDlqQueue]>(
          QueueName.ClickhouseWriterDlqQueue,
          {
            connection: newRedis,
            prefix: getQueuePrefix(QueueName.ClickhouseWriterDlqQueue),
            defaultJobOptions: {
              removeOnComplete: true,
              removeOnFail: 100_000,
              attempts: 10,
              backoff: {
                type: "exponential",
                delay: 30_000,
              },
            },
          },
        )
      : null;

    ClickhouseWriterDlqQueue.instance?.on("error", (err) => {
      logger.error("ClickhouseWriterDlqQueue error", err);
    });

    return ClickhouseWriterDlqQueue.instance;
  }
}
