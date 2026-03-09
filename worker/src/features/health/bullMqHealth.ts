export type BullMqQueueDepthSource = "ok" | "timeout" | "error" | "skipped";
export type BullMqQueueHealthStatus = "healthy" | "unhealthy";

export interface BullMqQueueRuntimeSnapshot {
  queueName: string;
  isRegistered: boolean;
  isRunning: boolean;
  registeredAt: number | null;
  lastReadyAt: number | null;
  lastActivityAt: number | null;
  lastCompletedAt: number | null;
  lastFailedAt: number | null;
  lastErrorAt: number | null;
  lastClosedAt: number | null;
  waitingCount: number | null;
  waitingCountSource: BullMqQueueDepthSource;
}

export interface BullMqQueueHealthSnapshot extends BullMqQueueRuntimeSnapshot {
  status: BullMqQueueHealthStatus;
  reason: string;
  inactivityMs: number | null;
}

export interface BullMqHealthCheckResponse {
  status: BullMqQueueHealthStatus;
  reason: string;
  staleAfterMs: number;
  checkedAt: string;
  queues: BullMqQueueHealthSnapshot[];
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

export const evaluateBullMqHealth = ({
  queueSnapshots,
  staleAfterMs,
  now = Date.now(),
}: {
  queueSnapshots: BullMqQueueRuntimeSnapshot[];
  staleAfterMs: number;
  now?: number;
}): BullMqHealthCheckResponse => {
  if (queueSnapshots.length === 0) {
    return {
      status: "healthy",
      reason: "no_critical_consumers_expected",
      staleAfterMs,
      checkedAt: new Date(now).toISOString(),
      queues: [],
    };
  }

  const queues = queueSnapshots.map((snapshot): BullMqQueueHealthSnapshot => {
    if (!snapshot.isRegistered) {
      return {
        ...snapshot,
        status: "unhealthy",
        reason: "consumer_not_registered",
        inactivityMs: null,
      };
    }

    const lastProgressAt = latestTimestamp(
      snapshot.lastActivityAt,
      snapshot.lastCompletedAt,
      snapshot.lastFailedAt,
      snapshot.lastReadyAt,
      snapshot.registeredAt,
    );
    const inactivityMs =
      lastProgressAt === null ? null : Math.max(0, now - lastProgressAt);

    if (!snapshot.isRunning) {
      return {
        ...snapshot,
        status: "unhealthy",
        reason: "consumer_not_running",
        inactivityMs,
      };
    }

    if (
      snapshot.lastClosedAt !== null &&
      (lastProgressAt === null || snapshot.lastClosedAt >= lastProgressAt)
    ) {
      return {
        ...snapshot,
        status: "unhealthy",
        reason: "consumer_closed",
        inactivityMs,
      };
    }

    if (
      snapshot.waitingCountSource === "ok" &&
      snapshot.waitingCount !== null &&
      snapshot.waitingCount > 0 &&
      inactivityMs !== null &&
      inactivityMs > staleAfterMs
    ) {
      const stalledReason =
        snapshot.lastErrorAt !== null &&
        lastProgressAt !== null &&
        snapshot.lastErrorAt >= lastProgressAt
          ? "consumer_error_with_backlog"
          : "backlog_without_recent_activity";

      return {
        ...snapshot,
        status: "unhealthy",
        reason: stalledReason,
        inactivityMs,
      };
    }

    return {
      ...snapshot,
      status: "healthy",
      reason:
        snapshot.waitingCountSource === "ok"
          ? "ok"
          : `queue_depth_${snapshot.waitingCountSource}`,
      inactivityMs,
    };
  });

  const firstUnhealthyQueue = queues.find(
    (queue) => queue.status === "unhealthy",
  );

  return {
    status: firstUnhealthyQueue ? "unhealthy" : "healthy",
    reason: firstUnhealthyQueue?.reason ?? "ok",
    staleAfterMs,
    checkedAt: new Date(now).toISOString(),
    queues,
  };
};
