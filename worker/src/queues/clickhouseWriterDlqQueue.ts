import { Job } from "bullmq";

import {
  logger,
  QueueName,
  TQueueJobTypes,
} from "@langfuse/shared/src/server";
import { ClickhouseWriter, TableName } from "../services/ClickhouseWriter";

export const clickhouseWriterDlqProcessor = async (
  job: Job<TQueueJobTypes[QueueName.ClickhouseWriterDlqQueue]>,
) => {
  const {
    tableName,
    record,
    projectId,
    verificationKey,
    originalAttempts,
  } = job.data.payload;

  logger.info("Replaying clickhouse writer DLQ record", {
    jobId: job.id,
    tableName,
    projectId,
    verificationKey,
    originalAttempts,
  });

  await ClickhouseWriter.getInstance().addToQueueAndWait(
    tableName as TableName,
    record as never,
  );

  logger.info("Finished replaying clickhouse writer DLQ record", {
    jobId: job.id,
    tableName,
    projectId,
    verificationKey,
  });

  return true;
};
