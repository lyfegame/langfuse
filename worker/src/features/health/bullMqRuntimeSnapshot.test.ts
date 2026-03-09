import { describe, expect, test, vi } from "vitest";

import type { WorkerHealthSnapshot } from "../../queues/workerManager";
import { buildBullMqQueueRuntimeSnapshot } from "./bullMqRuntimeSnapshot";

const NOW = 1_000_000;
const PROCESS_STARTED_AT = 900_000;
const STARTUP_GRACE_MS = 90_000;
const STALE_AFTER_MS = 120_000;

const makeWorkerSnapshot = (
  overrides: Partial<WorkerHealthSnapshot> = {},
): WorkerHealthSnapshot => ({
  queueName: "ingestion-queue",
  isRunning: true,
  registeredAt: 700_000,
  lastReadyAt: 710_000,
  lastActivityAt: 950_000,
  lastCompletedAt: 950_000,
  lastFailedAt: null,
  lastErrorAt: null,
  lastClosedAt: null,
  ...overrides,
});

describe("buildBullMqQueueRuntimeSnapshot", () => {
  test("uses startup grace when worker registration is still warming up", async () => {
    const getQueueWaitingCount = vi.fn();

    const result = await buildBullMqQueueRuntimeSnapshot({
      queueName: "ingestion-queue",
      workerSnapshot: null,
      now: PROCESS_STARTED_AT + 30_000,
      processStartedAt: PROCESS_STARTED_AT,
      startupGraceMs: STARTUP_GRACE_MS,
      staleAfterMs: STALE_AFTER_MS,
      getQueueWaitingCount,
    });

    expect(result).toEqual({
      queueName: "ingestion-queue",
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
    });
    expect(getQueueWaitingCount).not.toHaveBeenCalled();
  });

  test("reports missing workers after startup grace expires", async () => {
    const getQueueWaitingCount = vi.fn();

    const result = await buildBullMqQueueRuntimeSnapshot({
      queueName: "ingestion-queue",
      workerSnapshot: null,
      now: PROCESS_STARTED_AT + STARTUP_GRACE_MS + 1,
      processStartedAt: PROCESS_STARTED_AT,
      startupGraceMs: STARTUP_GRACE_MS,
      staleAfterMs: STALE_AFTER_MS,
      getQueueWaitingCount,
    });

    expect(result.isRegistered).toBe(false);
    expect(result.isRunning).toBe(false);
    expect(result.waitingCountSource).toBe("skipped");
    expect(getQueueWaitingCount).not.toHaveBeenCalled();
  });

  test("skips queue depth lookup for recently active consumers", async () => {
    const getQueueWaitingCount = vi.fn();

    const result = await buildBullMqQueueRuntimeSnapshot({
      queueName: "ingestion-queue",
      workerSnapshot: makeWorkerSnapshot(),
      now: NOW,
      processStartedAt: PROCESS_STARTED_AT,
      startupGraceMs: STARTUP_GRACE_MS,
      staleAfterMs: STALE_AFTER_MS,
      getQueueWaitingCount,
    });

    expect(result.waitingCount).toBeNull();
    expect(result.waitingCountSource).toBe("skipped");
    expect(getQueueWaitingCount).not.toHaveBeenCalled();
  });

  test("checks queue depth for stale consumers", async () => {
    const getQueueWaitingCount = vi.fn().mockResolvedValue({
      waitingCount: 3,
      waitingCountSource: "ok",
    });

    const result = await buildBullMqQueueRuntimeSnapshot({
      queueName: "ingestion-queue",
      workerSnapshot: makeWorkerSnapshot({
        lastActivityAt: NOW - 200_000,
        lastCompletedAt: NOW - 200_000,
      }),
      now: NOW,
      processStartedAt: PROCESS_STARTED_AT,
      startupGraceMs: STARTUP_GRACE_MS,
      staleAfterMs: STALE_AFTER_MS,
      getQueueWaitingCount,
    });

    expect(getQueueWaitingCount).toHaveBeenCalledWith("ingestion-queue");
    expect(result.waitingCount).toBe(3);
    expect(result.waitingCountSource).toBe("ok");
  });
});
