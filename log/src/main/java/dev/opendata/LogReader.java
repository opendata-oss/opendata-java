package dev.opendata;

import java.io.Closeable;
import java.util.List;

/**
 * Java binding for reading from an OpenData LogDb.
 *
 * <p>Provides sequential read access to log entries for a given key.
 * This is a thin wrapper over the native Rust LogDbReader.
 */
public class LogReader implements Closeable {

    private final long handle;
    private volatile boolean closed = false;

    private LogReader(long handle) {
        this.handle = handle;
    }

    /**
     * Creates a new LogReader for the given log.
     *
     * @param logHandle the native handle of the parent LogDb
     * @return a new LogReader instance
     */
    static LogReader create(long logHandle) {
        long handle = nativeCreate(logHandle);
        if (handle == 0) {
            throw new RuntimeException("Failed to create LogReader instance");
        }
        return new LogReader(handle);
    }

    /**
     * Reads up to maxEntries from the log for the given key.
     *
     * <p>Returns immediately with available entries, which may be fewer than requested
     * or empty if no new entries are available.
     *
     * <p>The native layer extracts the timestamp header from each entry's value,
     * returning the original payload without the header.
     *
     * @param key           the key to read from
     * @param startSequence the sequence number to start reading from
     * @param maxEntries    maximum number of entries to read
     * @return list of log entries (may be empty)
     */
    public List<LogEntry> read(byte[] key, long startSequence, int maxEntries) {
        checkNotClosed();
        LogEntry[] entries = nativeRead(handle, key, startSequence, maxEntries);
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
            throw new IllegalStateException("LogReader is closed");
        }
    }

    // Native methods
    private static native long nativeCreate(long logHandle);
    private static native LogEntry[] nativeRead(long handle, byte[] key, long startSequence, long maxEntries);
    private static native void nativeClose(long handle);
}
