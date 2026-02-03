package dev.opendata;

import java.io.Closeable;

/**
 * Java binding for the OpenData LogDb trait.
 *
 * <p>Provides append-only log operations backed by a native Rust implementation.
 * This is a thin wrapper over the native layer - callers are responsible for
 * batching and backpressure.
 */
public class LogDb implements Closeable {

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
     * Appends a batch of records to the log.
     *
     * <p>This is a blocking call that returns when all records have been persisted.
     * For better throughput, batch multiple records into a single call.
     *
     * @param records the records to append
     * @return the result of the append operation (sequence of first record)
     */
    public AppendResult append(Record[] records) {
        checkNotClosed();
        return nativeAppend(handle, records);
    }

    /**
     * Appends a single record to the log.
     *
     * <p>Convenience method for single-record appends. For better throughput,
     * prefer {@link #append(Record[])} with batched records.
     *
     * @param key   the key to append under
     * @param value the value to append
     * @return the result of the append operation
     */
    public AppendResult append(byte[] key, byte[] value) {
        return append(new Record[]{new Record(key, value)});
    }

    /**
     * Creates a reader for this log.
     *
     * @return a new LogReader instance
     */
    public LogReader reader() {
        checkNotClosed();
        return LogReader.create(handle);
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

    private static native AppendResult nativeAppend(long handle, Record[] records);

    private static native void nativeClose(long handle);
}
