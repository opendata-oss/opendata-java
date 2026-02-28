package dev.opendata;

import dev.opendata.common.AppendTimeoutException;
import dev.opendata.common.OpenDataNativeException;
import dev.opendata.common.QueueFullException;
import dev.opendata.ffi.Native;
import dev.opendata.ffi.opendata_log_config_t;
import dev.opendata.ffi.opendata_log_reader_config_t;
import dev.opendata.ffi.opendata_log_seq_bound_t;
import dev.opendata.ffi.opendata_log_seq_range_t;
import dev.opendata.ffi.opendata_log_result_t;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.charset.StandardCharsets;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;

import dev.opendata.common.ObjectStoreConfig;

/**
 * Panama FFM interop layer for the opendata-log C library.
 *
 * <p>This class provides the bridge between Java and the native C library,
 * handling memory management, error checking, and data marshaling. It follows
 * the same patterns as slatedb-java's NativeInterop.
 *
 * <h2>Timestamp Header</h2>
 * <p>The Java layer prepends an 8-byte big-endian timestamp to each value
 * before passing it to the C library. On read, the timestamp is extracted
 * and the original payload is returned. This enables end-to-end latency
 * measurement for benchmarking (e.g., openmessaging benchmark).
 *
 * <pre>
 * ┌─────────────────────┬──────────────────────┐
 * │ timestamp_ms (8B)   │ original payload     │
 * │ big-endian i64      │                      │
 * └─────────────────────┴──────────────────────┘
 * </pre>
 */
final class NativeInterop {

    static final int TIMESTAMP_HEADER_SIZE = 8;

    // Result kind codes from the C API (opendata_log_result_t.kind)
    private static final int OPENDATA_LOG_OK = 0;
    private static final int OPENDATA_LOG_ERROR_QUEUE_FULL = 5;
    private static final int OPENDATA_LOG_ERROR_TIMEOUT = 6;

    private NativeInterop() {
    }

    // =========================================================================
    // Handle classes
    // =========================================================================

    static abstract class NativeHandle implements AutoCloseable {

        private final String handleType;
        private final AtomicBoolean closed = new AtomicBoolean(false);
        private volatile MemorySegment segment;

        NativeHandle(String handleType, MemorySegment segment) {
            this.handleType = Objects.requireNonNull(handleType, "handleType");
            this.segment = requireNativeHandle(segment, handleType);
        }

        final MemorySegment segment() {
            MemorySegment current = segment;
            if (closed.get() || current.equals(MemorySegment.NULL)) {
                throw new IllegalStateException(handleType + " is closed");
            }
            return current;
        }

        final boolean isClosed() {
            return closed.get();
        }

        @Override
        public final void close() {
            if (!closed.compareAndSet(false, true)) {
                return;
            }
            MemorySegment current = segment;
            try {
                closeNative(current);
            } finally {
                segment = MemorySegment.NULL;
            }
        }

        protected abstract void closeNative(MemorySegment segment);
    }

    static final class ObjectStoreHandle extends NativeHandle {
        ObjectStoreHandle(MemorySegment segment) {
            super("ObjectStore", segment);
        }

        @Override
        protected void closeNative(MemorySegment segment) {
            try (Arena arena = Arena.ofConfined()) {
                checkResult(Native.opendata_log_object_store_close(arena, segment));
            }
        }
    }

    static final class LogHandle extends NativeHandle {
        LogHandle(MemorySegment segment) {
            super("Log", segment);
        }

        @Override
        protected void closeNative(MemorySegment segment) {
            try (Arena arena = Arena.ofConfined()) {
                checkResult(Native.opendata_log_close(arena, segment));
            }
        }
    }

    static final class ReaderHandle extends NativeHandle {
        ReaderHandle(MemorySegment segment) {
            super("Reader", segment);
        }

        @Override
        protected void closeNative(MemorySegment segment) {
            try (Arena arena = Arena.ofConfined()) {
                checkResult(Native.opendata_log_reader_close(arena, segment));
            }
        }
    }

    static final class IteratorHandle extends NativeHandle {
        IteratorHandle(MemorySegment segment) {
            super("Iterator", segment);
        }

        @Override
        protected void closeNative(MemorySegment segment) {
            try (Arena arena = Arena.ofConfined()) {
                checkResult(Native.opendata_log_iterator_close(arena, segment));
            }
        }
    }

    // =========================================================================
    // Iterator result
    // =========================================================================

    record RawIteratorResult(
            boolean present, long sequence, long timestamp,
            MemorySegment keyPtr, long keyLen,
            MemorySegment valuePtr, long valueLen
    ) {
    }

    // =========================================================================
    // Object store factory methods
    // =========================================================================

    static ObjectStoreHandle objectStoreInMemory() {
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment outStore = arena.allocate(Native.C_POINTER);
            checkResult(Native.opendata_log_object_store_in_memory(arena, outStore));
            return new ObjectStoreHandle(outStore.get(Native.C_POINTER, 0));
        }
    }

    static ObjectStoreHandle objectStoreLocal(String path) {
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment outStore = arena.allocate(Native.C_POINTER);
            MemorySegment nativePath = marshalCString(arena, path);
            checkResult(Native.opendata_log_object_store_local(arena, nativePath, outStore));
            return new ObjectStoreHandle(outStore.get(Native.C_POINTER, 0));
        }
    }

    static ObjectStoreHandle objectStoreAws(String region, String bucket) {
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment outStore = arena.allocate(Native.C_POINTER);
            MemorySegment nativeRegion = marshalCString(arena, region);
            MemorySegment nativeBucket = marshalCString(arena, bucket);
            checkResult(Native.opendata_log_object_store_aws(arena, nativeRegion, nativeBucket, outStore));
            return new ObjectStoreHandle(outStore.get(Native.C_POINTER, 0));
        }
    }

    static ObjectStoreHandle resolveObjectStore(ObjectStoreConfig config) {
        return switch (config) {
            case ObjectStoreConfig.InMemory() -> objectStoreInMemory();
            case ObjectStoreConfig.Aws aws -> objectStoreAws(aws.region(), aws.bucket());
            case ObjectStoreConfig.Local local -> objectStoreLocal(local.path());
        };
    }

    // =========================================================================
    // Log operations
    // =========================================================================

    static LogHandle logOpen(int storageType, String slatedbPath,
                             MemorySegment objectStore, String settingsPath,
                             long sealIntervalMs) {
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment config = opendata_log_config_t.allocate(arena);
            opendata_log_config_t.storage_type(config, (byte) storageType);
            opendata_log_config_t.slatedb_path(config, marshalNullableCString(arena, slatedbPath));
            opendata_log_config_t.object_store(config, objectStore);
            opendata_log_config_t.settings_path(config, marshalNullableCString(arena, settingsPath));
            opendata_log_config_t.seal_interval_ms(config, sealIntervalMs);

            MemorySegment outLog = arena.allocate(Native.C_POINTER);
            checkResult(Native.opendata_log_open(arena, config, outLog));
            return new LogHandle(outLog.get(Native.C_POINTER, 0));
        }
    }

    static void logFlush(LogHandle handle) {
        try (Arena arena = Arena.ofConfined()) {
            checkResult(Native.opendata_log_flush(arena, handle.segment()));
        }
    }

    static AppendResult logTryAppend(LogHandle handle, Record[] records) {
        return doAppend(handle.segment(), records, (arena, seg, keys, keyLens, vals, valLens, count, outSeq) ->
                Native.opendata_log_try_append(arena, seg, keys, keyLens, vals, valLens, count, outSeq));
    }

    static AppendResult logTryAppend(LogHandle handle, RecordBatch batch) {
        return doAppendBatch(handle.segment(), batch, (arena, seg, keys, keyLens, vals, valLens, count, outSeq) ->
                Native.opendata_log_try_append(arena, seg, keys, keyLens, vals, valLens, count, outSeq));
    }

    static AppendResult logAppendTimeout(LogHandle handle, Record[] records, long timeoutMs) {
        return doAppend(handle.segment(), records, (arena, seg, keys, keyLens, vals, valLens, count, outSeq) ->
                Native.opendata_log_append_timeout(arena, seg, keys, keyLens, vals, valLens, count, timeoutMs, outSeq));
    }

    static AppendResult logAppendTimeout(LogHandle handle, RecordBatch batch, long timeoutMs) {
        return doAppendBatch(handle.segment(), batch, (arena, seg, keys, keyLens, vals, valLens, count, outSeq) ->
                Native.opendata_log_append_timeout(arena, seg, keys, keyLens, vals, valLens, count, timeoutMs, outSeq));
    }

    static IteratorHandle logScan(LogHandle handle, byte[] key, long startSequence) {
        return doScan(handle.segment(), key, startSequence, Native::opendata_log_scan);
    }

    // =========================================================================
    // Reader operations
    // =========================================================================

    static ReaderHandle readerOpen(int storageType, String slatedbPath,
                                   MemorySegment objectStore, String settingsPath,
                                   long refreshIntervalMs) {
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment config = opendata_log_reader_config_t.allocate(arena);
            opendata_log_reader_config_t.storage_type(config, (byte) storageType);
            opendata_log_reader_config_t.slatedb_path(config, marshalNullableCString(arena, slatedbPath));
            opendata_log_reader_config_t.object_store(config, objectStore);
            opendata_log_reader_config_t.settings_path(config, marshalNullableCString(arena, settingsPath));
            opendata_log_reader_config_t.refresh_interval_ms(config, refreshIntervalMs);

            MemorySegment outReader = arena.allocate(Native.C_POINTER);
            checkResult(Native.opendata_log_reader_open(arena, config, outReader));
            return new ReaderHandle(outReader.get(Native.C_POINTER, 0));
        }
    }

    static IteratorHandle readerScan(ReaderHandle handle, byte[] key, long startSequence) {
        return doScan(handle.segment(), key, startSequence, Native::opendata_log_reader_scan);
    }

    // =========================================================================
    // Iterator operations
    // =========================================================================

    static RawIteratorResult iteratorNextRaw(IteratorHandle handle) {
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment outPresent = arena.allocate(ValueLayout.JAVA_BOOLEAN);
            MemorySegment outKey = arena.allocate(Native.C_POINTER);
            MemorySegment outKeyLen = arena.allocate(Native.C_LONG);
            MemorySegment outSequence = arena.allocate(ValueLayout.JAVA_LONG);
            MemorySegment outValue = arena.allocate(Native.C_POINTER);
            MemorySegment outValueLen = arena.allocate(Native.C_LONG);

            checkResult(Native.opendata_log_iterator_next(arena, handle.segment(),
                    outPresent, outKey, outKeyLen, outSequence, outValue, outValueLen));

            boolean present = outPresent.get(ValueLayout.JAVA_BOOLEAN, 0);
            if (!present) {
                return new RawIteratorResult(false, 0, 0,
                        MemorySegment.NULL, 0, MemorySegment.NULL, 0);
            }

            long sequence = outSequence.get(ValueLayout.JAVA_LONG, 0);
            MemorySegment keyPtr = outKey.get(Native.C_POINTER, 0);
            long keyLen = outKeyLen.get(Native.C_LONG, 0);
            MemorySegment valuePtr = outValue.get(Native.C_POINTER, 0);
            long valueLen = outValueLen.get(Native.C_LONG, 0);

            // Extract timestamp from the value segment header
            long timestamp = 0;
            if (valueLen >= TIMESTAMP_HEADER_SIZE) {
                MemorySegment valueSeg = valuePtr.reinterpret(valueLen);
                timestamp = Long.reverseBytes(
                        valueSeg.get(ValueLayout.JAVA_LONG_UNALIGNED, 0));
            }

            return new RawIteratorResult(true, sequence, timestamp,
                    keyPtr, keyLen, valuePtr, valueLen);
        }
    }

    static void freeBytes(MemorySegment ptr, long len) {
        if (!ptr.equals(MemorySegment.NULL) && len > 0) {
            Native.opendata_log_bytes_free(ptr, len);
        }
    }

    // =========================================================================
    // Shared append / scan helpers
    // =========================================================================

    @FunctionalInterface
    private interface AppendCall {
        MemorySegment invoke(Arena arena, MemorySegment handle,
                             MemorySegment keys, MemorySegment keyLens,
                             MemorySegment values, MemorySegment valueLens,
                             long count, MemorySegment outSeq);
    }

    private static AppendResult doAppend(MemorySegment handle, Record[] records, AppendCall call) {
        int count = records.length;
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment nativeKeys = arena.allocate(Native.C_POINTER, count);
            MemorySegment keyLens = arena.allocate(Native.C_LONG, count);
            MemorySegment nativeValues = arena.allocate(Native.C_POINTER, count);
            MemorySegment valueLens = arena.allocate(Native.C_LONG, count);

            marshalRecords(arena, records, nativeKeys, keyLens, nativeValues, valueLens);

            MemorySegment outSeq = arena.allocate(ValueLayout.JAVA_LONG);
            checkResult(call.invoke(arena, handle,
                    nativeKeys, keyLens, nativeValues, valueLens, count, outSeq));

            long startSeq = outSeq.get(ValueLayout.JAVA_LONG, 0);
            return new AppendResult(startSeq, records[0].timestampMs());
        }
    }

    private static AppendResult doAppendBatch(MemorySegment handle, RecordBatch batch, AppendCall call) {
        int count = batch.count();
        if (count == 0) {
            throw new IllegalArgumentException("batch must not be empty");
        }
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment nativeKeys = arena.allocate(Native.C_POINTER, count);
            MemorySegment keyLens = arena.allocate(Native.C_LONG, count);
            MemorySegment nativeValues = arena.allocate(Native.C_POINTER, count);
            MemorySegment valueLens = arena.allocate(Native.C_LONG, count);

            long[] batchKeyOffsets = batch.keyOffsets();
            long[] batchKeyLengths = batch.keyLengths();
            long[] batchValueOffsets = batch.valueOffsets();
            long[] batchValueLengths = batch.valueLengths();
            MemorySegment batchKeysData = batch.keysData();
            MemorySegment batchValuesData = batch.valuesData();

            for (int i = 0; i < count; i++) {
                nativeKeys.setAtIndex(Native.C_POINTER, i,
                        batchKeysData.asSlice(batchKeyOffsets[i], batchKeyLengths[i]));
                keyLens.setAtIndex(Native.C_LONG, i, batchKeyLengths[i]);
                nativeValues.setAtIndex(Native.C_POINTER, i,
                        batchValuesData.asSlice(batchValueOffsets[i], batchValueLengths[i]));
                valueLens.setAtIndex(Native.C_LONG, i, batchValueLengths[i]);
            }

            MemorySegment outSeq = arena.allocate(ValueLayout.JAVA_LONG);
            checkResult(call.invoke(arena, handle,
                    nativeKeys, keyLens, nativeValues, valueLens, count, outSeq));

            long startSeq = outSeq.get(ValueLayout.JAVA_LONG, 0);
            return new AppendResult(startSeq, batch.firstTimestampMs());
        }
    }

    @FunctionalInterface
    private interface ScanCall {
        MemorySegment invoke(Arena arena, MemorySegment handle,
                             MemorySegment key, long keyLen,
                             MemorySegment seqRange, MemorySegment outIterator);
    }

    private static IteratorHandle doScan(MemorySegment handle, byte[] key,
                                          long startSequence, ScanCall call) {
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment nativeKey = marshalBytes(arena, key);
            MemorySegment seqRange = marshalSeqRange(arena, startSequence);
            MemorySegment outIterator = arena.allocate(Native.C_POINTER);
            checkResult(call.invoke(arena, handle, nativeKey, key.length, seqRange, outIterator));
            return new IteratorHandle(outIterator.get(Native.C_POINTER, 0));
        }
    }

    // =========================================================================
    // Memory marshaling helpers
    // =========================================================================

    private static MemorySegment marshalBytes(Arena arena, byte[] bytes) {
        Objects.requireNonNull(bytes, "bytes");
        if (bytes.length == 0) {
            return MemorySegment.NULL;
        }
        MemorySegment nativeBytes = arena.allocate(bytes.length, 1);
        MemorySegment.copy(MemorySegment.ofArray(bytes), 0, nativeBytes, 0, bytes.length);
        return nativeBytes;
    }

    private static MemorySegment marshalCString(Arena arena, String value) {
        Objects.requireNonNull(value, "value");
        byte[] utf8 = value.getBytes(StandardCharsets.UTF_8);
        MemorySegment nativeString = arena.allocate(utf8.length + 1L, 1);
        if (utf8.length > 0) {
            MemorySegment.copy(MemorySegment.ofArray(utf8), 0, nativeString, 0, utf8.length);
        }
        nativeString.set(ValueLayout.JAVA_BYTE, utf8.length, (byte) 0);
        return nativeString;
    }

    private static MemorySegment marshalNullableCString(Arena arena, String value) {
        if (value == null) {
            return MemorySegment.NULL;
        }
        return marshalCString(arena, value);
    }

    private static void marshalRecords(Arena arena, Record[] records,
                                       MemorySegment nativeKeys, MemorySegment keyLens,
                                       MemorySegment nativeValues, MemorySegment valueLens) {
        for (int i = 0; i < records.length; i++) {
            byte[] key = records[i].key();
            MemorySegment keyData = marshalBytes(arena, key);
            nativeKeys.setAtIndex(Native.C_POINTER, i, keyData);
            keyLens.setAtIndex(Native.C_LONG, i, key.length);

            // Write timestamp header + value directly into arena, avoiding intermediate byte[]
            byte[] value = records[i].value();
            long totalLen = TIMESTAMP_HEADER_SIZE + value.length;
            MemorySegment valueData = arena.allocate(totalLen, 1);
            valueData.set(ValueLayout.JAVA_LONG_UNALIGNED, 0,
                    Long.reverseBytes(records[i].timestampMs())); // big-endian
            MemorySegment.copy(MemorySegment.ofArray(value), 0,
                    valueData, TIMESTAMP_HEADER_SIZE, value.length);
            nativeValues.setAtIndex(Native.C_POINTER, i, valueData);
            valueLens.setAtIndex(Native.C_LONG, i, totalLen);
        }
    }

    private static MemorySegment marshalSeqRange(Arena arena, long startSequence) {
        MemorySegment seqRange = opendata_log_seq_range_t.allocate(arena);

        MemorySegment start = opendata_log_seq_range_t.start(seqRange);
        opendata_log_seq_bound_t.kind(start, (byte) Native.OPENDATA_LOG_BOUND_INCLUDED());
        opendata_log_seq_bound_t.value(start, startSequence);

        MemorySegment end = opendata_log_seq_range_t.end(seqRange);
        opendata_log_seq_bound_t.kind(end, (byte) Native.OPENDATA_LOG_BOUND_UNBOUNDED());
        opendata_log_seq_bound_t.value(end, 0);

        return seqRange;
    }

    // =========================================================================
    // Error handling
    // =========================================================================

    private static MemorySegment requireNativeHandle(MemorySegment segment, String handleType) {
        if (segment == null || segment.equals(MemorySegment.NULL)) {
            throw new IllegalStateException("Failed to create " + handleType + " handle: null pointer");
        }
        return segment;
    }

    private static void checkResult(MemorySegment result) {
        int kindCode = opendata_log_result_t.kind(result);

        if (kindCode == OPENDATA_LOG_OK) {
            Native.opendata_log_result_free(result);
            return;
        }

        String message;
        try {
            MemorySegment msgPtr = opendata_log_result_t.message(result);
            if (!msgPtr.equals(MemorySegment.NULL)) {
                message = msgPtr.reinterpret(Long.MAX_VALUE).getString(0, StandardCharsets.UTF_8);
            } else {
                message = "Unknown error (kind=" + kindCode + ")";
            }
        } finally {
            Native.opendata_log_result_free(result);
        }

        throw mapError(kindCode, message);
    }

    static RuntimeException mapError(int kindCode, String message) {
        if (kindCode == OPENDATA_LOG_ERROR_QUEUE_FULL) {
            return new QueueFullException(message);
        } else if (kindCode == OPENDATA_LOG_ERROR_TIMEOUT) {
            return new AppendTimeoutException(message);
        } else {
            return new OpenDataNativeException(message);
        }
    }
}
