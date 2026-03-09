import type { WorkerHealthSnapshot } from "../../queues/workerManager";
import type {
  BullMqQueueDepthSource,
  BullMqQueueRuntimeSnapshot,
} from "./bullMqHealth";

export interface BullMqQueueDepthLookupResult {
  waitingCount: number | null;
  waitingCountSource: BullMqQueueDepthSource;
}

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

export const buildBullMqQueueRuntimeSnapshot = async ({
  queueName,
  workerSnapshot,
  now,
  processStartedAt,
  startupGraceMs,
  staleAfterMs,
  getQueueWaitingCount,
}: {
  queueName: string;
  workerSnapshot: WorkerHealthSnapshot | null;
  now: number;
  processStartedAt: number;
  startupGraceMs: number;
  staleAfterMs: number;
  getQueueWaitingCount: (
    queueName: string,
  ) => Promise<BullMqQueueDepthLookupResult>;
}): Promise<BullMqQueueRuntimeSnapshot> => {
  if (!workerSnapshot) {
    if (now - processStartedAt <= startupGraceMs) {
      return {
        queueName,
        isRegistered: true,
        isRunning: true,
        registeredAt: processStartedAt,
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
    lastProgressAt === null || now - lastProgressAt > staleAfterMs;
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
