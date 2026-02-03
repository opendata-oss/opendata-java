package dev.opendata.common;

/**
 * Top-level storage configuration.
 *
 * <p>This sealed interface represents the different storage backends
 * available for OpenData systems.
 */
public sealed interface StorageConfig
        permits StorageConfig.InMemory, StorageConfig.SlateDb {

    /**
     * In-memory storage (fast, no persistence).
     *
     * <p>Useful for testing and development. Data is lost when the process exits.
     */
    record InMemory() implements StorageConfig {}

    /**
     * SlateDB-backed storage (persistent).
     *
     * @param path         path prefix for SlateDB data in the object store
     * @param objectStore  object store provider configuration
     * @param settingsPath optional path to SlateDB settings file (TOML/YAML/JSON)
     */
    record SlateDb(
            String path,
            ObjectStoreConfig objectStore,
            String settingsPath
    ) implements StorageConfig {

        /**
         * Creates a SlateDb config with default settings path (null).
         *
         * @param path        path prefix for SlateDB data
         * @param objectStore object store provider configuration
         */
        public SlateDb(String path, ObjectStoreConfig objectStore) {
            this(path, objectStore, null);
        }

        public SlateDb {
            if (path == null || path.isBlank()) {
                throw new IllegalArgumentException("path must not be null or blank");
            }
            if (objectStore == null) {
                throw new IllegalArgumentException("objectStore must not be null");
            }
        }
    }
}
