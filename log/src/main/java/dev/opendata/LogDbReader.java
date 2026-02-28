package dev.opendata;

import dev.opendata.common.StorageConfig;

import java.io.Closeable;

/**
 * A read-only view of the log.
 *
 * <p>LogDbReader provides access to all read operations via the {@link LogRead}
 * interface, but not write operations. Unlike {@link LogDb} which has write access,
 * LogDbReader opens storage independently and can coexist with a separate LogDb writer.
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
 * LogDbReaderConfig config = new LogDbReaderConfig(new StorageConfig.SlateDb(...));
 * try (LogDbReader reader = LogDbReader.open(config)) {
 *     try (LogScanIterator iter = reader.scan(key, 0)) {
 *         for (LogEntry entry : iter) {
 *             process(entry);
 *         }
 *     }
 * }
 * }</pre>
 */
public class LogDbReader implements Closeable, LogRead {

    private final NativeInterop.ReaderHandle handle;

    private LogDbReader(NativeInterop.ReaderHandle handle) {
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

        StorageConfig storage = config.storage();
        long refreshIntervalMs = config.refreshIntervalMs() != null
                ? config.refreshIntervalMs()
                : -1;

        switch (storage) {
            case StorageConfig.InMemory() -> {
                try (NativeInterop.ObjectStoreHandle objectStore = NativeInterop.objectStoreInMemory()) {
                    NativeInterop.ReaderHandle readerHandle = NativeInterop.readerOpen(
                            0, null, objectStore.segment(), null, refreshIntervalMs);
                    return new LogDbReader(readerHandle);
                }
            }
            case StorageConfig.SlateDb slateDb -> {
                try (NativeInterop.ObjectStoreHandle objectStore = NativeInterop.resolveObjectStore(slateDb.objectStore())) {
                    NativeInterop.ReaderHandle readerHandle = NativeInterop.readerOpen(
                            1, slateDb.path(), objectStore.segment(), slateDb.settingsPath(), refreshIntervalMs);
                    return new LogDbReader(readerHandle);
                }
            }
        }
    }

    @Override
    public LogScanRawIterator scanRaw(byte[] key, long startSequence) {
        if (handle.isClosed()) {
            throw new IllegalStateException("LogDbReader is closed");
        }
        return new LogScanRawIterator(NativeInterop.readerScan(handle, key, startSequence));
    }

    @Override
    public void close() {
        handle.close();
    }
}
