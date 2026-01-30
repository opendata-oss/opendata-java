package io.opendata.log;

import java.io.Closeable;
import java.util.List;

/**
 * Java binding for reading from an OpenData Log.
 *
 * <p>Provides sequential read access to log entries for a given key.
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
     * @param logHandle the native handle of the parent Log
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
     * Reads up to maxEntries from the log for the configured key.
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

    /**
     * Reads entries, polling until at least one entry is available or timeout expires.
     *
     * <p>Note: This is a polling implementation. True blocking reads will be
     * available once the upstream push API is implemented.
     *
     * @param key           the key to read from
     * @param startSequence the sequence number to start reading from
     * @param maxEntries    maximum number of entries to read
     * @param timeoutMs     maximum time to wait in milliseconds
     * @return list of log entries (may be empty if timeout expires)
     */
    public List<LogEntry> readBlocking(byte[] key, long startSequence, int maxEntries, long timeoutMs) {
        checkNotClosed();
        long deadline = System.currentTimeMillis() + timeoutMs;
        long currentSeq = startSequence;

        while (System.currentTimeMillis() < deadline) {
            List<LogEntry> entries = read(key, currentSeq, maxEntries);
            if (!entries.isEmpty()) {
                return entries;
            }
            // Brief sleep before retry
            try {
                Thread.sleep(Math.min(10, deadline - System.currentTimeMillis()));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return List.of();
            }
        }
        return List.of();
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
