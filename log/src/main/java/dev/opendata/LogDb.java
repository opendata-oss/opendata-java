package dev.opendata;

import dev.opendata.common.AppendTimeoutException;
import dev.opendata.common.QueueFullException;

import java.io.Closeable;
import java.util.List;

/**
 * Java binding for the OpenData LogDb trait.
 *
 * <p>Provides append-only log operations backed by a native Rust implementation.
 * This is a thin wrapper over the native layer - callers are responsible for
 * batching and backpressure.
 *
 * <p>Implements {@link LogRead} for read operations. For read-only access without
 * write capabilities, use {@link LogDbReader} instead.
 */
public class LogDb implements Closeable, LogRead {

    static {
        System.loadLibrary("opendata_log_jni");
    }

    private final long handle;
    private volatile boolean closed = false;

    private LogDb(long handle) {
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
        long handle = nativeCreate(config);
        if (handle == 0) {
            throw new RuntimeException("Failed to create LogDb instance");
        }
        return new LogDb(handle);
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
        return nativeTryAppend(handle, records);
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
        return nativeAppendTimeout(handle, records, timeoutMs);
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
    public List<LogEntry> scan(byte[] key, long startSequence, int maxEntries) {
        checkNotClosed();
        LogEntry[] entries = nativeScan(handle, key, startSequence, maxEntries);
        return entries != null ? List.of(entries) : List.of();
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
        nativeFlush(handle);
    }

    @Override
    public void close() {
        if (!closed) {
            closed = true;
            nativeClose(handle);
        }
    }

    private void checkNotClosed() {
        if (closed) {
            throw new IllegalStateException("LogDb is closed");
        }
    }

    long getHandle() {
        return handle;
    }

    // Native methods
    private static native long nativeCreate(LogDbConfig config);
    private static native AppendResult nativeTryAppend(long handle, Record[] records);
    private static native AppendResult nativeAppendTimeout(long handle, Record[] records, long timeoutMs);
    private static native LogEntry[] nativeScan(long handle, byte[] key, long startSequence, long maxEntries);
    private static native void nativeFlush(long handle);
    private static native void nativeClose(long handle);
}
