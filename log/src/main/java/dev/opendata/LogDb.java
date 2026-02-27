package dev.opendata;

import dev.opendata.common.AppendTimeoutException;
import dev.opendata.common.ObjectStoreConfig;
import dev.opendata.common.QueueFullException;
import dev.opendata.common.StorageConfig;

import java.io.Closeable;

/**
 * Java binding for the OpenData LogDb trait.
 *
 * <p>Provides append-only log operations backed by a native C implementation
 * via Panama FFM. This is a thin wrapper over the native layer - callers are
 * responsible for batching and backpressure.
 *
 * <p>Implements {@link LogRead} for read operations. For read-only access without
 * write capabilities, use {@link LogDbReader} instead.
 */
public class LogDb implements Closeable, LogRead {

    private final NativeInterop.LogHandle handle;

    private LogDb(NativeInterop.LogHandle handle) {
        this.handle = handle;
    }

    /**
     * Opens a LogDb instance with the specified configuration.
     *
     * @param config the log configuration
     * @return a new LogDb instance
     */
    public static LogDb open(LogDbConfig config) {
        if (config == null) {
            throw new IllegalArgumentException("config must not be null");
        }

        StorageConfig storage = config.storage();
        long sealIntervalMs = config.segmentation().sealIntervalMs() != null
                ? config.segmentation().sealIntervalMs()
                : -1;

        switch (storage) {
            case StorageConfig.InMemory() -> {
                try (NativeInterop.ObjectStoreHandle objectStore = NativeInterop.objectStoreInMemory()) {
                    NativeInterop.LogHandle logHandle = NativeInterop.logOpen(
                            0, null, objectStore.segment(), null, sealIntervalMs);
                    return new LogDb(logHandle);
                }
            }
            case StorageConfig.SlateDb slateDb -> {
                try (NativeInterop.ObjectStoreHandle objectStore = resolveObjectStore(slateDb.objectStore())) {
                    NativeInterop.LogHandle logHandle = NativeInterop.logOpen(
                            1, slateDb.path(), objectStore.segment(), slateDb.settingsPath(), sealIntervalMs);
                    return new LogDb(logHandle);
                }
            }
        }
    }

    /**
     * Opens a LogDb instance with in-memory storage (for testing).
     *
     * @return a new LogDb instance
     */
    public static LogDb openInMemory() {
        return open(LogDbConfig.inMemory());
    }

    /**
     * Appends a batch of records without blocking for queue space.
     *
     * <p>Fails immediately with {@link QueueFullException} if the write queue is full.
     * The caller can retry the same {@code records} array after backpressure clears.
     *
     * @param records the records to append
     * @return the result of the append operation (sequence of first record)
     * @throws QueueFullException if the write queue is full
     */
    public AppendResult tryAppend(Record[] records) {
        checkNotClosed();
        return NativeInterop.logTryAppend(handle, records);
    }

    /**
     * Appends a single record without blocking for queue space.
     *
     * <p>Convenience method for single-record appends. For better throughput,
     * prefer {@link #tryAppend(Record[])} with batched records.
     *
     * @param key   the key to append under
     * @param value the value to append
     * @return the result of the append operation
     * @throws QueueFullException if the write queue is full
     */
    public AppendResult tryAppend(byte[] key, byte[] value) {
        return tryAppend(new Record[]{new Record(key, value)});
    }

    /**
     * Appends a batch of records, blocking up to {@code timeoutMs} for queue space.
     *
     * <p>If the write queue does not drain within the deadline, throws
     * {@link AppendTimeoutException}. The caller can retry the same {@code records}
     * array.
     *
     * @param records   the records to append
     * @param timeoutMs maximum time to wait in milliseconds
     * @return the result of the append operation (sequence of first record)
     * @throws AppendTimeoutException if the timeout expires before queue space is available
     * @throws QueueFullException     if the write queue is full (unlikely with timeout)
     */
    public AppendResult appendTimeout(Record[] records, long timeoutMs) {
        checkNotClosed();
        return NativeInterop.logAppendTimeout(handle, records, timeoutMs);
    }

    /**
     * Appends a single record, blocking up to {@code timeoutMs} for queue space.
     *
     * @param key       the key to append under
     * @param value     the value to append
     * @param timeoutMs maximum time to wait in milliseconds
     * @return the result of the append operation
     * @throws AppendTimeoutException if the timeout expires before queue space is available
     * @throws QueueFullException     if the write queue is full (unlikely with timeout)
     */
    public AppendResult appendTimeout(byte[] key, byte[] value, long timeoutMs) {
        return appendTimeout(new Record[]{new Record(key, value)}, timeoutMs);
    }

    @Override
    public LogScanIterator scan(byte[] key, long startSequence) {
        checkNotClosed();
        return new LogScanIterator(NativeInterop.logScan(handle, key, startSequence));
    }

    /**
     * Flushes all pending writes to durable storage.
     *
     * <p>This ensures that all writes that have been acknowledged are persisted
     * to durable storage. For SlateDB-backed storage, this flushes the memtable
     * to the WAL and object store.
     */
    public void flush() {
        checkNotClosed();
        NativeInterop.logFlush(handle);
    }

    @Override
    public void close() {
        handle.close();
    }

    private void checkNotClosed() {
        if (handle.isClosed()) {
            throw new IllegalStateException("LogDb is closed");
        }
    }

    static NativeInterop.ObjectStoreHandle resolveObjectStore(ObjectStoreConfig config) {
        return switch (config) {
            case ObjectStoreConfig.InMemory() -> NativeInterop.objectStoreInMemory();
            case ObjectStoreConfig.Aws aws -> NativeInterop.objectStoreAws(aws.region(), aws.bucket());
            case ObjectStoreConfig.Local local -> NativeInterop.objectStoreLocal(local.path());
        };
    }
}
