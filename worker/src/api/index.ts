import express from "express";
import { logger, traceException } from "@langfuse/shared/src/server";

import {
  checkBullMqHealth,
  checkContainerReadiness,
} from "../features/health";
import type { WorkerHealthResponse } from "../features/health";

const router = express.Router();

router.get<{}, WorkerHealthResponse>("/health", async (_req, res) => {
  try {
    await checkBullMqHealth(res);
  } catch (e) {
    traceException(e);
    logger.error("Health check failed", e);
    res.status(500).json({
      status: "error",
    });
  }
});

router.get<{}, { status: string }>("/ready", async (_req, res) => {
  try {
    await checkContainerReadiness(res, true);
  } catch (e) {
    traceException(e);
    logger.error("Readiness check failed", e);
    res.status(500).json({
      status: "error",
    });
  }
});

export default router;
