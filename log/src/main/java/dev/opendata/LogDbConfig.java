package dev.opendata;

import dev.opendata.common.StorageConfig;

/**
 * Configuration for opening a {@link LogDb}.
 *
 * <p>This record holds all the settings needed to initialize a log instance,
 * including storage backend configuration, segmentation settings, and read visibility.
 *
 * @param storage        storage backend configuration
 * @param segmentation   segmentation configuration
 * @param readVisibility controls which data is visible to reads
 */
public record LogDbConfig(
        StorageConfig storage,
        SegmentConfig segmentation,
        ReadVisibility readVisibility
) {

    /**
     * Creates a config with the specified storage, default segmentation, and default read visibility.
     *
     * @param storage storage backend configuration
     */
    public LogDbConfig(StorageConfig storage) {
        this(storage, SegmentConfig.DEFAULT, ReadVisibility.MEMORY);
    }

    /**
     * Creates a config with the specified storage and segmentation, and default read visibility.
     *
     * @param storage      storage backend configuration
     * @param segmentation segmentation configuration
     */
    public LogDbConfig(StorageConfig storage, SegmentConfig segmentation) {
        this(storage, segmentation, ReadVisibility.MEMORY);
    }

    public LogDbConfig {
        if (storage == null) {
            throw new IllegalArgumentException("storage must not be null");
        }
        if (segmentation == null) {
            throw new IllegalArgumentException("segmentation must not be null");
        }
        if (readVisibility == null) {
            throw new IllegalArgumentException("readVisibility must not be null");
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
