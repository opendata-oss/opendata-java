package dev.opendata;

import dev.opendata.common.StorageConfig;

/**
 * Configuration for opening a {@link LogDbReader}.
 *
 * <p>This record holds settings for read-only log access, including storage
 * backend configuration and automatic refresh settings.
 *
 * @param storage           storage backend configuration
 * @param refreshIntervalMs interval in milliseconds for discovering new log data
 *                          written by other processes; null to use native default
 */
public record LogDbReaderConfig(
        StorageConfig storage,
        Long refreshIntervalMs
) {

    /**
     * Creates a config with the specified storage and native default refresh interval.
     *
     * @param storage storage backend configuration
     */
    public LogDbReaderConfig(StorageConfig storage) {
        this(storage, null);
    }

    public LogDbReaderConfig {
        if (storage == null) {
            throw new IllegalArgumentException("storage must not be null");
        }
        if (refreshIntervalMs != null && refreshIntervalMs <= 0) {
            throw new IllegalArgumentException("refreshIntervalMs must be positive");
        }
    }

    /**
     * Creates a default in-memory configuration for testing.
     *
     * @return a new LogDbReaderConfig with in-memory storage
     */
    public static LogDbReaderConfig inMemory() {
        return new LogDbReaderConfig(new StorageConfig.InMemory());
    }
}
