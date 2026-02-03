package dev.opendata.common;

/**
 * Object store provider configuration for SlateDB storage.
 *
 * <p>This sealed interface represents the different object store backends
 * that can be used with SlateDB storage.
 */
public sealed interface ObjectStoreConfig
        permits ObjectStoreConfig.InMemory, ObjectStoreConfig.Aws, ObjectStoreConfig.Local {

    /**
     * In-memory object store (useful for testing and development).
     */
    record InMemory() implements ObjectStoreConfig {}

    /**
     * AWS S3 object store.
     *
     * @param region AWS region (e.g., "us-west-2")
     * @param bucket S3 bucket name
     */
    record Aws(String region, String bucket) implements ObjectStoreConfig {
        public Aws {
            if (region == null || region.isBlank()) {
                throw new IllegalArgumentException("region must not be null or blank");
            }
            if (bucket == null || bucket.isBlank()) {
                throw new IllegalArgumentException("bucket must not be null or blank");
            }
        }
    }

    /**
     * Local filesystem object store.
     *
     * @param path path to the local directory for storage
     */
    record Local(String path) implements ObjectStoreConfig {
        public Local {
            if (path == null || path.isBlank()) {
                throw new IllegalArgumentException("path must not be null or blank");
            }
        }
    }
}
