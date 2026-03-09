import { describe, expect, test } from "vitest";

import { evaluateBullMqHealth } from "./bullMqHealth";
import type { BullMqQueueRuntimeSnapshot } from "./bullMqHealth";

const now = 1_000_000;

const makeSnapshot = (
  overrides: Partial<BullMqQueueRuntimeSnapshot> = {},
): BullMqQueueRuntimeSnapshot => ({
  queueName: "ingestion-queue",
  isRegistered: true,
  isRunning: true,
  registeredAt: 100_000,
  lastReadyAt: 120_000,
  lastActivityAt: 900_000,
  lastCompletedAt: 900_000,
  lastFailedAt: null,
  lastErrorAt: null,
  lastClosedAt: null,
  waitingCount: 0,
  waitingCountSource: "ok",
  ...overrides,
});

describe("evaluateBullMqHealth", () => {
  test("returns healthy when no critical consumers are expected", () => {
    const result = evaluateBullMqHealth({
      queueSnapshots: [],
      staleAfterMs: 120_000,
      now,
    });

    expect(result.status).toBe("healthy");
    expect(result.reason).toBe("no_critical_consumers_expected");
    expect(result.queues).toEqual([]);
  });

  test("fails when an expected consumer is not registered", () => {
    const result = evaluateBullMqHealth({
      queueSnapshots: [
        makeSnapshot({
          isRegistered: false,
          isRunning: false,
          registeredAt: null,
          lastReadyAt: null,
          lastActivityAt: null,
          lastCompletedAt: null,
        }),
      ],
      staleAfterMs: 120_000,
      now,
    });

    expect(result.status).toBe("unhealthy");
    expect(result.reason).toBe("consumer_not_registered");
    expect(result.queues[0]?.reason).toBe("consumer_not_registered");
  });

  test("fails when backlog exists without recent activity", () => {
    const result = evaluateBullMqHealth({
      queueSnapshots: [
        makeSnapshot({
          lastActivityAt: 700_000,
          lastCompletedAt: 700_000,
          waitingCount: 3,
        }),
      ],
      staleAfterMs: 120_000,
      now,
    });

    expect(result.status).toBe("unhealthy");
    expect(result.reason).toBe("backlog_without_recent_activity");
    expect(result.queues[0]?.inactivityMs).toBe(300_000);
  });

  test("stays healthy when backlog exists but activity is recent", () => {
    const result = evaluateBullMqHealth({
      queueSnapshots: [
        makeSnapshot({
          lastActivityAt: 950_000,
          lastCompletedAt: 950_000,
          waitingCount: 2,
        }),
      ],
      staleAfterMs: 120_000,
      now,
    });

    expect(result.status).toBe("healthy");
    expect(result.reason).toBe("ok");
    expect(result.queues[0]?.reason).toBe("ok");
  });

  test("does not fail liveness when queue depth lookup is unavailable", () => {
    const result = evaluateBullMqHealth({
      queueSnapshots: [
        makeSnapshot({
          waitingCount: null,
          waitingCountSource: "timeout",
          lastActivityAt: 100_000,
          lastCompletedAt: 100_000,
        }),
      ],
      staleAfterMs: 120_000,
      now,
    });

    expect(result.status).toBe("healthy");
    expect(result.reason).toBe("ok");
    expect(result.queues[0]?.reason).toBe("queue_depth_timeout");
  });

  test("surfaces a closed consumer as unhealthy", () => {
    const result = evaluateBullMqHealth({
      queueSnapshots: [
        makeSnapshot({
          lastActivityAt: 800_000,
          lastCompletedAt: 800_000,
          lastClosedAt: 900_000,
        }),
      ],
      staleAfterMs: 120_000,
      now,
    });

    expect(result.status).toBe("unhealthy");
    expect(result.reason).toBe("consumer_closed");
  });
});
