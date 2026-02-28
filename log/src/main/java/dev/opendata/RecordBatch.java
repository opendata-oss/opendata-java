package dev.opendata;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.Arrays;
import java.util.Objects;

/**
 * A builder that accumulates records into contiguous native memory segments,
 * eliminating per-append copy overhead when writing to {@link LogDb}.
 *
 * <p>Records are written into two arena-backed {@link MemorySegment}s — one for
 * keys and one for values. Values are stored with an 8-byte big-endian timestamp
 * header already prepended, matching the format expected by the native layer.
 *
 * <p>Typical usage:
 * <pre>{@code
 * try (RecordBatch batch = RecordBatch.create()) {
 *     for (...) {
 *         batch.add(key, value);
 *     }
 *     AppendResult result = log.tryAppend(batch);
 * }
 * }</pre>
 *
 * <p>The batch is <b>not</b> closed by append methods — the caller retains ownership.
 */
public final class RecordBatch implements AutoCloseable {

    private static final long DEFAULT_DATA_CAPACITY = 4096;
    private static final int DEFAULT_RECORD_CAPACITY = 64;

    private final Arena arena;

    private MemorySegment keysData;
    private long keysOffset;
    private MemorySegment valuesData;
    private long valuesOffset;

    private long[] keyOffsets;
    private long[] keyLengths;
    private long[] valueOffsets;
    private long[] valueLengths;

    private int count;
    private long firstTimestampMs;
    private boolean closed;

    private RecordBatch(Arena arena, long dataCapacity, int recordCapacity) {
        this.arena = arena;
        this.keysData = arena.allocate(dataCapacity, 1);
        this.valuesData = arena.allocate(dataCapacity, 1);
        this.keyOffsets = new long[recordCapacity];
        this.keyLengths = new long[recordCapacity];
        this.valueOffsets = new long[recordCapacity];
        this.valueLengths = new long[recordCapacity];
    }

    /**
     * Creates a new batch with default capacities.
     */
    public static RecordBatch create() {
        return create(DEFAULT_DATA_CAPACITY, DEFAULT_RECORD_CAPACITY);
    }

    /**
     * Creates a new batch with specified capacities.
     *
     * @param dataCapacity   initial byte capacity for key and value segments
     * @param recordCapacity initial number of records the batch can hold before growing
     */
    public static RecordBatch create(long dataCapacity, int recordCapacity) {
        if (dataCapacity <= 0) {
            throw new IllegalArgumentException("dataCapacity must be positive");
        }
        if (recordCapacity <= 0) {
            throw new IllegalArgumentException("recordCapacity must be positive");
        }
        return new RecordBatch(Arena.ofConfined(), dataCapacity, recordCapacity);
    }

    /**
     * Adds a record with the current wall-clock time as timestamp.
     */
    public void add(byte[] key, byte[] value) {
        add(key, value, System.currentTimeMillis());
    }

    /**
     * Adds a record with an explicit timestamp.
     *
     * @param key         the record key
     * @param value       the record value payload
     * @param timestampMs wall-clock time (epoch millis)
     */
    public void add(byte[] key, byte[] value, long timestampMs) {
        checkNotClosed();
        Objects.requireNonNull(key, "key must not be null");
        Objects.requireNonNull(value, "value must not be null");

        // Grow record arrays if needed
        if (count == keyOffsets.length) {
            int newCap = keyOffsets.length * 2;
            keyOffsets = Arrays.copyOf(keyOffsets, newCap);
            keyLengths = Arrays.copyOf(keyLengths, newCap);
            valueOffsets = Arrays.copyOf(valueOffsets, newCap);
            valueLengths = Arrays.copyOf(valueLengths, newCap);
        }

        // Copy key into keys segment
        long keyLen = key.length;
        keysData = ensureCapacity(keysData, keysOffset, keyLen);
        MemorySegment.copy(MemorySegment.ofArray(key), 0, keysData, keysOffset, keyLen);
        keyOffsets[count] = keysOffset;
        keyLengths[count] = keyLen;
        keysOffset += keyLen;

        // Copy timestamp header + value into values segment
        long totalValueLen = NativeInterop.TIMESTAMP_HEADER_SIZE + value.length;
        valuesData = ensureCapacity(valuesData, valuesOffset, totalValueLen);
        valuesData.set(ValueLayout.JAVA_LONG_UNALIGNED, valuesOffset,
                Long.reverseBytes(timestampMs));
        if (value.length > 0) {
            MemorySegment.copy(MemorySegment.ofArray(value), 0,
                    valuesData, valuesOffset + NativeInterop.TIMESTAMP_HEADER_SIZE, value.length);
        }
        valueOffsets[count] = valuesOffset;
        valueLengths[count] = totalValueLen;
        valuesOffset += totalValueLen;

        if (count == 0) {
            firstTimestampMs = timestampMs;
        }
        count++;
    }

    /**
     * Returns the number of records in this batch.
     */
    public int count() {
        return count;
    }

    /**
     * Returns {@code true} if this batch contains no records.
     */
    public boolean isEmpty() {
        return count == 0;
    }

    /**
     * Returns the timestamp of the first record added, or 0 if empty.
     */
    public long firstTimestampMs() {
        return firstTimestampMs;
    }

    @Override
    public void close() {
        if (!closed) {
            closed = true;
            arena.close();
        }
    }

    // Package-private accessors for NativeInterop
    MemorySegment keysData() {
        return keysData;
    }

    long[] keyOffsets() {
        return keyOffsets;
    }

    long[] keyLengths() {
        return keyLengths;
    }

    MemorySegment valuesData() {
        return valuesData;
    }

    long[] valueOffsets() {
        return valueOffsets;
    }

    long[] valueLengths() {
        return valueLengths;
    }

    private void checkNotClosed() {
        if (closed) {
            throw new IllegalStateException("RecordBatch is closed");
        }
    }

    /**
     * Ensures the segment has enough room for {@code needed} more bytes starting at {@code offset}.
     * If not, allocates a 2x segment and copies existing data.
     */
    private MemorySegment ensureCapacity(MemorySegment segment, long offset, long needed) {
        long required = offset + needed;
        if (required <= segment.byteSize()) {
            return segment;
        }
        long newSize = segment.byteSize();
        while (newSize < required) {
            newSize *= 2;
        }
        MemorySegment grown = arena.allocate(newSize, 1);
        MemorySegment.copy(segment, 0, grown, 0, offset);
        return grown;
    }
}
