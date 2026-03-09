import { Job, Processor, Worker, WorkerOptions } from "bullmq";
import {
  getQueue,
  convertQueueNameToMetricName,
  createNewRedisInstance,
  getQueuePrefix,
  logger,
  QueueName,
  IngestionQueue,
  TraceUpsertQueue,
  OtelIngestionQueue,
  recordGauge,
  recordHistogram,
  recordIncrement,
  redisQueueRetryOptions,
  traceException,
} from "@langfuse/shared/src/server";

export interface WorkerHealthSnapshot {
  queueName: string;
  isRunning: boolean;
  registeredAt: number;
  lastReadyAt: number | null;
  lastActivityAt: number | null;
  lastCompletedAt: number | null;
  lastFailedAt: number | null;
  lastErrorAt: number | null;
  lastClosedAt: number | null;
}

type WorkerHealthRecord = Omit<WorkerHealthSnapshot, "isRunning">;

export class WorkerManager {
  private static workers: { [key: string]: Worker } = {};
  private static workerHealth: { [key: string]: WorkerHealthRecord } = {};

  private static metricWrapper(
    processor: Processor,
    queueName: string,
  ): Processor {
    return async (job: Job) => {
      const startTime = Date.now();
      const waitTime = Date.now() - job.timestamp;
      recordIncrement(convertQueueNameToMetricName(queueName) + ".request");
      recordHistogram(
        convertQueueNameToMetricName(queueName) + ".wait_time",
        waitTime,
        {
          unit: "milliseconds",
        },
      );
      const result = await processor(job);
      const queue = queueName.startsWith(QueueName.IngestionQueue)
        ? IngestionQueue.getInstance({ shardName: queueName })
        : queueName.startsWith(QueueName.TraceUpsert)
          ? TraceUpsertQueue.getInstance({ shardName: queueName })
          : queueName.startsWith(QueueName.OtelIngestionQueue)
            ? OtelIngestionQueue.getInstance({ shardName: queueName })
            : getQueue(
                queueName as Exclude<
                  QueueName,
                  | QueueName.IngestionQueue
                  | QueueName.TraceUpsert
                  | QueueName.OtelIngestionQueue
                >,
              );
      Promise.allSettled([
        queue?.getWaitingCount().then((count) => {
          recordGauge(convertQueueNameToMetricName(queueName) + ".length", count, {
            unit: "records",
          });
        }),
        queue?.getFailedCount().then((count) => {
          recordGauge(
            convertQueueNameToMetricName(queueName) + ".dlq_length",
            count,
            {
              unit: "records",
            },
          );
        }),
      ]).catch((err) => {
        logger.error("Failed to record queue length", err);
      });
      recordHistogram(
        convertQueueNameToMetricName(queueName) + ".processing_time",
        Date.now() - startTime,
        { unit: "milliseconds" },
      );
      return result;
    };
  }

  private static ensureWorkerHealthRecord(queueName: string): WorkerHealthRecord {
    if (!WorkerManager.workerHealth[queueName]) {
      WorkerManager.workerHealth[queueName] = {
        queueName,
        registeredAt: Date.now(),
        lastReadyAt: null,
        lastActivityAt: null,
        lastCompletedAt: null,
        lastFailedAt: null,
        lastErrorAt: null,
        lastClosedAt: null,
      };
    }

    return WorkerManager.workerHealth[queueName];
  }

  private static updateWorkerHealth(
    queueName: string,
    update: Partial<WorkerHealthRecord>,
  ) {
    WorkerManager.workerHealth[queueName] = {
      ...WorkerManager.ensureWorkerHealthRecord(queueName),
      ...update,
    };
  }

  public static async closeWorkers(): Promise<void> {
    await Promise.allSettled(
      Object.values(WorkerManager.workers).map((worker) => worker.close()),
    );
    WorkerManager.workers = {};
    WorkerManager.workerHealth = {};
    logger.info("All workers have been closed.");
  }

  public static getWorker(queueName: string): Worker | undefined {
    return WorkerManager.workers[queueName];
  }

  public static getWorkerHealthSnapshot(
    queueName: string,
  ): WorkerHealthSnapshot | null {
    const workerHealth = WorkerManager.workerHealth[queueName];

    if (!workerHealth) {
      return null;
    }

    return {
      ...workerHealth,
      isRunning: WorkerManager.workers[queueName]?.isRunning() ?? false,
    };
  }

  public static register(
    queueName: string,
    processor: Processor,
    additionalOptions: Partial<WorkerOptions> = {},
  ): void {
    if (WorkerManager.workers[queueName]) {
      logger.info(`Worker ${queueName} is already registered`);
      return;
    }

    const redisInstance = createNewRedisInstance(redisQueueRetryOptions);
    if (!redisInstance) {
      logger.error("Failed to initialize redis connection");
      return;
    }

    const worker = new Worker(
      queueName,
      WorkerManager.metricWrapper(processor, queueName),
      {
        connection: redisInstance,
        prefix: getQueuePrefix(queueName),
        ...additionalOptions,
      },
    );
    WorkerManager.workers[queueName] = worker;
    WorkerManager.ensureWorkerHealthRecord(queueName);
    logger.info(`${queueName} executor started: ${worker.isRunning()}`);

    worker.on("ready", () => {
      WorkerManager.updateWorkerHealth(queueName, {
        lastReadyAt: Date.now(),
      });
    });

    worker.on("active", () => {
      WorkerManager.updateWorkerHealth(queueName, {
        lastActivityAt: Date.now(),
      });
    });

    worker.on("completed", () => {
      const now = Date.now();
      WorkerManager.updateWorkerHealth(queueName, {
        lastActivityAt: now,
        lastCompletedAt: now,
      });
    });

    worker.on("failed", (job: Job | undefined, err: Error) => {
      const now = Date.now();
      WorkerManager.updateWorkerHealth(queueName, {
        lastActivityAt: now,
        lastFailedAt: now,
      });
      logger.error(
        `Queue job ${job?.name} with id ${job?.id} in ${queueName} failed`,
        err,
      );
      traceException(err);
      recordIncrement(convertQueueNameToMetricName(queueName) + ".failed");
    });

    worker.on("error", (failedReason: Error) => {
      WorkerManager.updateWorkerHealth(queueName, {
        lastErrorAt: Date.now(),
      });
      logger.error(
        `Queue job ${queueName} errored: ${failedReason}`,
        failedReason,
      );
      traceException(failedReason);
      recordIncrement(convertQueueNameToMetricName(queueName) + ".error");
    });

    worker.on("closed", () => {
      WorkerManager.updateWorkerHealth(queueName, {
        lastClosedAt: Date.now(),
      });
    });
  }
}
