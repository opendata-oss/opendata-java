package dev.opendata;

import java.util.List;

/**
 * Interface for read operations on the log.
 *
 * <p>This interface defines the common read API shared by {@link LogDb} and
 * {@link LogDbReader}. It provides methods for reading entries from the log.
 *
 * <p>Implementors:
 * <ul>
 *   <li>{@link LogDb} - The main log interface with both read and write access.
 *   <li>{@link LogDbReader} - A read-only view of the log.
 * </ul>
 */
public interface LogRead {

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
    List<LogEntry> scan(byte[] key, long startSequence, int maxEntries);
}
