import { expect, it, describe } from "vitest";
import { Readable } from "stream";
import { createParquetValidationStream } from "../features/blobstorage/parquetValidation";

/**
 * Consume a stream fully and return the concatenated buffer.
 */
async function consumeStream(stream: Readable): Promise<Buffer> {
  const chunks: Buffer[] = [];
  for await (const chunk of stream) {
    chunks.push(Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk));
  }
  return Buffer.concat(chunks);
}

/**
 * Build a minimal valid Parquet-like buffer: PAR1 magic + padding to exceed 1 KiB.
 */
function buildValidParquetBuffer(size = 2048): Buffer {
  const buf = Buffer.alloc(size);
  // Write PAR1 magic as first 4 bytes
  buf[0] = 0x50; // P
  buf[1] = 0x41; // A
  buf[2] = 0x52; // R
  buf[3] = 0x31; // 1
  return buf;
}

describe("createParquetValidationStream", () => {
  it("should pass validation for a valid Parquet stream (PAR1 magic, > 1 KiB)", async () => {
    const data = buildValidParquetBuffer(2048);
    const source = Readable.from([data]);

    const { stream, validate } = createParquetValidationStream(source);
    const output = await consumeStream(stream);

    // Data passes through unchanged
    expect(output.equals(data)).toBe(true);

    // Validation should not throw
    expect(() => validate()).not.toThrow();
  });

  it("should reject a ClickHouse error response (no PAR1 magic, < 1 KiB)", async () => {
    // Simulate a ClickHouse error message uploaded as .parquet
    const errorText =
      "Code: 241. DB::Exception: Memory limit (total) exceeded: " +
      "would use 20.01 GiB (attempt to allocate chunk of 134217728 bytes), " +
      "maximum: 20.00 GiB. (MEMORY_LIMIT_EXCEEDED)";
    const source = Readable.from([Buffer.from(errorText)]);

    const { stream, validate } = createParquetValidationStream(source);
    await consumeStream(stream);

    expect(() => validate()).toThrow(/Invalid Parquet file/);
    expect(() => validate()).toThrow(/PAR1 magic/);
  });

  it("should reject an empty stream (0 bytes)", async () => {
    const source = Readable.from([]);

    const { stream, validate } = createParquetValidationStream(source);
    await consumeStream(stream);

    expect(() => validate()).toThrow(/Invalid Parquet file/);
  });

  it("should reject a file with PAR1 magic but under minimum size", async () => {
    // Valid magic but only 512 bytes — too small to be a real Parquet file
    const data = buildValidParquetBuffer(512);
    const source = Readable.from([data]);

    const { stream, validate } = createParquetValidationStream(source);
    await consumeStream(stream);

    expect(() => validate()).toThrow(/Invalid Parquet file/);
  });

  it("should reject a file that is exactly 1 KiB", async () => {
    // Requirement is strictly greater than 1 KiB.
    const data = buildValidParquetBuffer(1024);
    const source = Readable.from([data]);

    const { stream, validate } = createParquetValidationStream(source);
    await consumeStream(stream);

    expect(() => validate()).toThrow(/Invalid Parquet file/);
  });

  it("should reject a large file without PAR1 magic", async () => {
    // 2 KiB of zeros — large enough but wrong magic
    const data = Buffer.alloc(2048);
    const source = Readable.from([data]);

    const { stream, validate } = createParquetValidationStream(source);
    await consumeStream(stream);

    expect(() => validate()).toThrow(/Invalid Parquet file/);
  });

  it("should handle data arriving in multiple small chunks", async () => {
    // Send PAR1 magic split across two 2-byte chunks, then bulk data
    const chunk1 = Buffer.from([0x50, 0x41]); // PA
    const chunk2 = Buffer.from([0x52, 0x31]); // R1
    const chunk3 = Buffer.alloc(2048); // bulk padding
    const source = Readable.from([chunk1, chunk2, chunk3]);

    const { stream, validate } = createParquetValidationStream(source);
    const output = await consumeStream(stream);

    // All data passes through
    expect(output.length).toBe(2052);

    // Validation should pass — magic bytes correctly reassembled
    expect(() => validate()).not.toThrow();
  });

  it("should pass data through without modification", async () => {
    const data = buildValidParquetBuffer(4096);
    // Fill with recognizable pattern after magic bytes
    for (let i = 4; i < data.length; i++) {
      data[i] = i % 256;
    }
    const source = Readable.from([data]);

    const { stream, validate } = createParquetValidationStream(source);
    const output = await consumeStream(stream);

    expect(output.equals(data)).toBe(true);
    expect(() => validate()).not.toThrow();
  });
});
