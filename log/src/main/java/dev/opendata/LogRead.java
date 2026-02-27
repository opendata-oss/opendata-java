package dev.opendata;

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
     * <p>Returns an iterator over available entries. The caller must close the
     * iterator when done to release native resources.
     *
     * @param key           the key to scan
     * @param startSequence the sequence number to start scanning from
     * @return an iterator over log entries (caller must close)
     */
    LogScanIterator scan(byte[] key, long startSequence);
}
