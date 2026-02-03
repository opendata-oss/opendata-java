package dev.opendata;

import dev.opendata.common.StorageConfig;

/**
 * Configuration for opening a {@link LogDb}.
 *
 * <p>This record holds all the settings needed to initialize a log instance,
 * including storage backend configuration and segmentation settings.
 *
 * @param storage      storage backend configuration
 * @param segmentation segmentation configuration
 */
public record LogDbConfig(
        StorageConfig storage,
        SegmentConfig segmentation
) {

    /**
     * Creates a config with the specified storage and default segmentation.
     *
     * @param storage storage backend configuration
     */
    public LogDbConfig(StorageConfig storage) {
        this(storage, SegmentConfig.DEFAULT);
    }

    public LogDbConfig {
        if (storage == null) {
            throw new IllegalArgumentException("storage must not be null");
        }
        if (segmentation == null) {
            throw new IllegalArgumentException("segmentation must not be null");
        }
    }

    /**
     * Creates a default in-memory configuration for testing.
     *
     * @return a new LogDbConfig with in-memory storage
     */
    public static LogDbConfig inMemory() {
        return new LogDbConfig(new StorageConfig.InMemory());
    }
}
