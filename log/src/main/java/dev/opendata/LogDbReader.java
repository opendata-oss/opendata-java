package dev.opendata;

import java.io.Closeable;
import java.util.List;

/**
 * A read-only view of the log.
 *
 * <p>LogDbReader provides access to all read operations via the {@link LogRead}
 * interface, but not write operations. Unlike {@link LogReader} which shares
 * storage with a parent {@link LogDb}, LogDbReader opens storage independently
 * and can coexist with a separate LogDb writer.
 *
 * <p>This is useful for:
 * <ul>
 *   <li>Consumers that should not have write access
 *   <li>Separating read and write concerns in your application
 *   <li>Benchmarking with realistic end-to-end latency (separate reader/writer)
 * </ul>
 *
 * <h2>Example</h2>
 * <pre>{@code
 * LogDbConfig config = new LogDbConfig(new StorageConfig.SlateDb(...));
 * try (LogDbReader reader = LogDbReader.open(config)) {
 *     List<LogEntry> entries = reader.read(key, 0, 100);
 *     for (LogEntry entry : entries) {
 *         process(entry);
 *     }
 * }
 * }</pre>
 */
public class LogDbReader implements Closeable, LogRead {

    static {
        System.loadLibrary("opendata_log_jni");
    }

    private final long handle;
    private volatile boolean closed = false;

    private LogDbReader(long handle) {
        this.handle = handle;
    }

    /**
     * Opens a read-only view of the log with the given configuration.
     *
     * <p>This creates a LogDbReader that can read entries but cannot append
     * new records. The reader opens storage independently and can coexist
     * with a separate LogDb writer.
     *
     * @param config the reader configuration
     * @return a new LogDbReader instance
     */
    public static LogDbReader open(LogDbReaderConfig config) {
        if (config == null) {
            throw new IllegalArgumentException("config must not be null");
        }
        long handle = nativeCreate(config);
        if (handle == 0) {
            throw new RuntimeException("Failed to create LogDbReader instance");
        }
        return new LogDbReader(handle);
    }

    /**
     * Scans entries from the log for the given key starting at a sequence number.
     *
     * <p>Returns immediately with available entries, which may be fewer than requested
     * or empty if no new entries are available.
     *
     * @param key           the key to scan
     * @param startSequence the sequence number to start scanning from
     * @param maxEntries    maximum number of entries to return
     * @return list of log entries (may be empty)
     */
    @Override
    public List<LogEntry> scan(byte[] key, long startSequence, int maxEntries) {
        checkNotClosed();
        LogEntry[] entries = nativeScan(handle, key, startSequence, maxEntries);
        return entries != null ? List.of(entries) : List.of();
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
            throw new IllegalStateException("LogDbReader is closed");
        }
    }

    // Native methods
    private static native long nativeCreate(LogDbReaderConfig config);
    private static native LogEntry[] nativeScan(long handle, byte[] key, long startSequence, long maxEntries);
    private static native void nativeClose(long handle);
}
