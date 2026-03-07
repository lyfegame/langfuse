import { expect, it, describe } from "vitest";
import { Readable } from "stream";
import {
  createParquetValidationStream,
  MIN_VALID_PARQUET_SIZE,
} from "../features/blobstorage/parquetValidation";

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
 * Build a minimal valid Parquet-like buffer: PAR1 magic at start and end,
 * plus a valid trailer (4-byte footer length + PAR1 magic).
 */
function buildValidParquetBuffer(size = 2048): Buffer {
  const buf = Buffer.alloc(size);
  // Write PAR1 magic as first 4 bytes
  buf[0] = 0x50; // P
  buf[1] = 0x41; // A
  buf[2] = 0x52; // R
  buf[3] = 0x31; // 1

  if (size >= 12) {
    // Write footer length (little endian). Must be > 0 and <= size - 12
    // (4-byte header + footer metadata + 4-byte footer length + 4-byte trailer).
    const footerLength = Math.min(64, size - 12);
    buf.writeUInt32LE(footerLength, size - 8);
    // Write PAR1 magic as last 4 bytes
    buf[size - 4] = 0x50; // P
    buf[size - 3] = 0x41; // A
    buf[size - 2] = 0x52; // R
    buf[size - 1] = 0x31; // 1
  }
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

  it("should reject a file with PAR1 header but invalid footer", async () => {
    const data = Buffer.alloc(2048);
    data[0] = 0x50; // P
    data[1] = 0x41; // A
    data[2] = 0x52; // R
    data[3] = 0x31; // 1
    // Footer intentionally left invalid (all zeros)
    const source = Readable.from([data]);

    const { stream, validate } = createParquetValidationStream(source);
    await consumeStream(stream);

    expect(() => validate()).toThrow(/Invalid Parquet file/);
  });

  it("should reject a file with PAR1 trailer but invalid footer length", async () => {
    const data = buildValidParquetBuffer(2048);
    // Corrupt footer length to exceed payload length.
    data.writeUInt32LE(10_000, 2040);
    const source = Readable.from([data]);

    const { stream, validate } = createParquetValidationStream(source);
    await consumeStream(stream);

    expect(() => validate()).toThrow(/Invalid Parquet file/);
  });

  it("should reject a footer length that overlaps with the header magic", async () => {
    // For a 2048-byte file, the max valid footer length is 2048 - 12 = 2036
    // (4 bytes header + footerLength bytes FileMetaData + 4 bytes footer len + 4 bytes trailer).
    // A footer length of 2037 would imply FileMetaData overlaps the PAR1 header.
    const data = buildValidParquetBuffer(2048);
    data.writeUInt32LE(2037, 2040); // 1 byte too large
    const source = Readable.from([data]);

    const { stream, validate } = createParquetValidationStream(source);
    await consumeStream(stream);

    expect(() => validate()).toThrow(/Invalid Parquet file/);
  });

  it("should accept a footer length that exactly fills the space between header and trailer", async () => {
    // Max valid footer length for 2048 bytes = 2048 - 12 = 2036
    const data = buildValidParquetBuffer(2048);
    data.writeUInt32LE(2036, 2040);
    const source = Readable.from([data]);

    const { stream, validate } = createParquetValidationStream(source);
    await consumeStream(stream);

    expect(() => validate()).not.toThrow();
  });

  it("should handle data arriving in multiple small chunks", async () => {
    // Send PAR1 magic split across two 2-byte chunks, then bulk data
    const chunk1 = Buffer.from([0x50, 0x41]); // PA
    const chunk2 = Buffer.from([0x52, 0x31]); // R1
    const chunk3 = Buffer.alloc(2048); // bulk padding + footer magic
    chunk3.writeUInt32LE(64, chunk3.length - 8);
    chunk3[chunk3.length - 4] = 0x50; // P
    chunk3[chunk3.length - 3] = 0x41; // A
    chunk3[chunk3.length - 2] = 0x52; // R
    chunk3[chunk3.length - 1] = 0x31; // 1
    const source = Readable.from([chunk1, chunk2, chunk3]);

    const { stream, validate } = createParquetValidationStream(source);
    const output = await consumeStream(stream);

    // All data passes through
    expect(output.length).toBe(2052);

    // Validation should pass — magic bytes correctly reassembled
    expect(() => validate()).not.toThrow();
  });

  it("should handle trailer split across the last two chunks", async () => {
    // Build a valid parquet buffer and split so the 8-byte trailer spans two chunks.
    const full = buildValidParquetBuffer(2048);
    const bodyChunk = full.subarray(0, 2045); // everything up to last 3 bytes
    const tailChunk = full.subarray(2045); // last 3 bytes (partial trailer)
    const source = Readable.from([bodyChunk, tailChunk]);

    const { stream, validate } = createParquetValidationStream(source);
    const output = await consumeStream(stream);

    expect(output.length).toBe(2048);
    expect(() => validate()).not.toThrow();
  });

  it("should pass data through without modification", async () => {
    const data = buildValidParquetBuffer(4096);
    // Fill with recognizable pattern while keeping header and trailer intact.
    for (let i = 4; i < data.length - 8; i++) {
      data[i] = i % 256;
    }
    const source = Readable.from([data]);

    const { stream, validate } = createParquetValidationStream(source);
    const output = await consumeStream(stream);

    expect(output.equals(data)).toBe(true);
    expect(() => validate()).not.toThrow();
  });

  it("should propagate source stream errors", async () => {
    const source = new Readable({
      read() {
        this.push(Buffer.from([0x50, 0x41, 0x52, 0x31]));
        this.destroy(new Error("stream failure"));
      },
    });

    const { stream } = createParquetValidationStream(source);
    await expect(consumeStream(stream)).rejects.toThrow("stream failure");
  });

  it("should honor custom minimum size threshold", async () => {
    const data = buildValidParquetBuffer(2048);
    const source = Readable.from([data]);

    const { stream, validate } = createParquetValidationStream(source, {
      minSizeBytes: 4096,
    });
    await consumeStream(stream);

    expect(() => validate()).toThrow(/Invalid Parquet file/);
    expect(MIN_VALID_PARQUET_SIZE).toBe(1024);
  });
});
