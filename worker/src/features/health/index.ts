import { prisma } from "@langfuse/shared/src/db";
import {
  IngestionQueue,
  logger,
  OtelIngestionQueue,
  QueueName,
  redis,
  SecondaryIngestionQueue,
} from "@langfuse/shared/src/server";
import type { Queue } from "bullmq";
import type { Response } from "express";

import { env } from "../../env";
import { WorkerManager } from "../../queues/workerManager";
import type { WorkerHealthSnapshot } from "../../queues/workerManager";
import { evaluateBullMqHealth } from "./bullMqHealth";
import type {
  BullMqHealthCheckResponse,
  BullMqQueueDepthSource,
  BullMqQueueRuntimeSnapshot,
} from "./bullMqHealth";

const READINESS_REDIS_TIMEOUT_MS = 2_000;
const BULLMQ_HEALTH_STALE_MS = 2 * 60 * 1000;
const BULLMQ_HEALTH_STARTUP_GRACE_MS = 90 * 1000;
const BULLMQ_HEALTH_QUEUE_DEPTH_TIMEOUT_MS = 500;
const BULLMQ_QUEUE_DEPTH_TIMEOUT_ERROR = "bullmq_queue_depth_timeout";
const PROCESS_STARTED_AT = Date.now();

export interface WorkerHealthResponse {
  status: string;
  bullmq?: BullMqHealthCheckResponse;
}

/**
 * Readiness checks can fail on SIGTERM to drain traffic before shutdown.
 */
export const checkContainerReadiness = async (
  res: Response<{ status: string }>,
  failOnSigterm: boolean,
) => {
  if (failOnSigterm && isSigtermReceived()) {
    logger.info(
      "Health check failed: SIGTERM / SIGINT received, shutting down.",
    );
    return res.status(500).json({
      status: "SIGTERM / SIGINT received, shutting down",
    });
  }

  await prisma.$queryRaw`SELECT 1;`;

  if (!redis) {
    throw new Error("Redis connection not available");
  }

  await withTimeout(
    redis.ping(),
    READINESS_REDIS_TIMEOUT_MS,
    "Redis ping timeout after 2 seconds",
  );

  return res.json({
    status: "ok",
  });
};

/**
 * Liveness stays process-local plus BullMQ-aware so it can safely drive restarts
 * without coupling to Prisma / Redis latency or temporary dependency blips.
 */
export const checkBullMqHealth = async (
  res: Response<WorkerHealthResponse>,
) => {
  if (isSigtermReceived()) {
    return res.json({
      status: "ok",
      bullmq: {
        status: "healthy",
        reason: "sigterm_received",
        staleAfterMs: BULLMQ_HEALTH_STALE_MS,
        checkedAt: new Date().toISOString(),
        queues: [],
      },
    });
  }

  const expectedQueueNames = getExpectedBullMqQueueNames();
  const queueSnapshots = await Promise.all(
    expectedQueueNames.map(async (queueName) =>
      buildBullMqQueueRuntimeSnapshot(
        queueName,
        WorkerManager.getWorkerHealthSnapshot(queueName),
      ),
    ),
  );

  const bullmq = evaluateBullMqHealth({
    queueSnapshots,
    staleAfterMs: BULLMQ_HEALTH_STALE_MS,
  });

  return res.status(bullmq.status === "healthy" ? 200 : 500).json({
    status: bullmq.status === "healthy" ? "ok" : "error",
    bullmq,
  });
};

const getExpectedBullMqQueueNames = (): string[] => {
  if (env.LANGFUSE_WORKER_ROLE === "export") {
    return [];
  }

  const queueNames: string[] = [];

  if (env.QUEUE_CONSUMER_INGESTION_QUEUE_IS_ENABLED === "true") {
    queueNames.push(...IngestionQueue.getShardNames());
  }

  if (env.QUEUE_CONSUMER_OTEL_INGESTION_QUEUE_IS_ENABLED === "true") {
    queueNames.push(...OtelIngestionQueue.getShardNames());
  }

  if (env.QUEUE_CONSUMER_INGESTION_SECONDARY_QUEUE_IS_ENABLED === "true") {
    queueNames.push(QueueName.IngestionSecondaryQueue);
  }

  return queueNames;
};

const latestTimestamp = (
  ...timestamps: Array<number | null | undefined>
): number | null => {
  const validTimestamps = timestamps.filter(
    (timestamp): timestamp is number => typeof timestamp === "number",
  );

  if (validTimestamps.length === 0) {
    return null;
  }

  return Math.max(...validTimestamps);
};

const getQueueForHealth = (queueName: string): Queue | null => {
  if (queueName.startsWith(QueueName.IngestionQueue)) {
    return IngestionQueue.getInstance({ shardName: queueName });
  }

  if (queueName.startsWith(QueueName.OtelIngestionQueue)) {
    return OtelIngestionQueue.getInstance({ shardName: queueName });
  }

  if (queueName === QueueName.IngestionSecondaryQueue) {
    return SecondaryIngestionQueue.getInstance();
  }

  return null;
};

const buildBullMqQueueRuntimeSnapshot = async (
  queueName: string,
  workerSnapshot: WorkerHealthSnapshot | null,
): Promise<BullMqQueueRuntimeSnapshot> => {
  const now = Date.now();

  if (!workerSnapshot) {
    if (now - PROCESS_STARTED_AT <= BULLMQ_HEALTH_STARTUP_GRACE_MS) {
      return {
        queueName,
        isRegistered: true,
        isRunning: true,
        registeredAt: PROCESS_STARTED_AT,
        lastReadyAt: null,
        lastActivityAt: null,
        lastCompletedAt: null,
        lastFailedAt: null,
        lastErrorAt: null,
        lastClosedAt: null,
        waitingCount: null,
        waitingCountSource: "skipped",
      };
    }

    return {
      queueName,
      isRegistered: false,
      isRunning: false,
      registeredAt: null,
      lastReadyAt: null,
      lastActivityAt: null,
      lastCompletedAt: null,
      lastFailedAt: null,
      lastErrorAt: null,
      lastClosedAt: null,
      waitingCount: null,
      waitingCountSource: "skipped",
    };
  }

  const lastProgressAt = latestTimestamp(
    workerSnapshot.lastActivityAt,
    workerSnapshot.lastCompletedAt,
    workerSnapshot.lastFailedAt,
    workerSnapshot.lastReadyAt,
    workerSnapshot.registeredAt,
  );
  const shouldCheckQueueDepth =
    lastProgressAt === null || now - lastProgressAt > BULLMQ_HEALTH_STALE_MS;
  const { waitingCount, waitingCountSource } = shouldCheckQueueDepth
    ? await getQueueWaitingCount(queueName)
    : { waitingCount: null, waitingCountSource: "skipped" as const };

  return {
    queueName,
    isRegistered: true,
    isRunning: workerSnapshot.isRunning,
    registeredAt: workerSnapshot.registeredAt,
    lastReadyAt: workerSnapshot.lastReadyAt,
    lastActivityAt: workerSnapshot.lastActivityAt,
    lastCompletedAt: workerSnapshot.lastCompletedAt,
    lastFailedAt: workerSnapshot.lastFailedAt,
    lastErrorAt: workerSnapshot.lastErrorAt,
    lastClosedAt: workerSnapshot.lastClosedAt,
    waitingCount,
    waitingCountSource,
  };
};

const getQueueWaitingCount = async (
  queueName: string,
): Promise<{
  waitingCount: number | null;
  waitingCountSource: BullMqQueueDepthSource;
}> => {
  const queue = getQueueForHealth(queueName);

  if (!queue) {
    return {
      waitingCount: null,
      waitingCountSource: "skipped",
    };
  }

  try {
    const waitingCount = await withTimeout(
      queue.getWaitingCount(),
      BULLMQ_HEALTH_QUEUE_DEPTH_TIMEOUT_MS,
      BULLMQ_QUEUE_DEPTH_TIMEOUT_ERROR,
    );

    return {
      waitingCount,
      waitingCountSource: "ok",
    };
  } catch (error) {
    const errorMessage = error instanceof Error ? error.message : "";

    return {
      waitingCount: null,
      waitingCountSource:
        errorMessage === BULLMQ_QUEUE_DEPTH_TIMEOUT_ERROR ? "timeout" : "error",
    };
  }
};

const withTimeout = async <T>(
  promise: Promise<T>,
  timeoutMs: number,
  timeoutMessage: string,
): Promise<T> =>
  Promise.race([
    promise,
    new Promise<T>((_, reject) =>
      setTimeout(() => reject(new Error(timeoutMessage)), timeoutMs),
    ),
  ]);

let sigtermReceived = false;

export const setSigtermReceived = () => {
  logger.info("Set sigterm received to true");
  sigtermReceived = true;
};

const isSigtermReceived = () => sigtermReceived;
