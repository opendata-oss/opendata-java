package dev.opendata;

import java.io.Closeable;
import java.util.concurrent.CompletableFuture;

/**
 * Java binding for the OpenData Log trait.
 *
 * <p>Provides append-only log operations backed by a native Rust implementation.
 */
public class Log implements Closeable {

    static {
        System.loadLibrary("opendata_log_jni");
    }

    private final long handle;
    private volatile boolean closed = false;

    private Log(long handle) {
        this.handle = handle;
    }

    /**
     * Opens a Log instance with in-memory storage (for testing).
     *
     * @param configPath unused, kept for backward compatibility
     * @return a new Log instance
     */
    public static Log open(String configPath) {
        return open(StorageType.IN_MEMORY, null, null, null, null, null);
    }

    /**
     * Opens a Log instance with the specified storage configuration.
     *
     * @param storageType   storage backend type
     * @param path          data path for SlateDB (ignored for in-memory)
     * @param objectStore   object store type: "in-memory", "local", or "s3"
     * @param s3Bucket      S3 bucket name (only for s3 object store)
     * @param s3Region      S3 region (only for s3 object store)
     * @return a new Log instance
     */
    public static Log open(StorageType storageType, String path,
                           String objectStore, String s3Bucket, String s3Region) {
        return open(storageType, path, objectStore, s3Bucket, s3Region, null);
    }

    /**
     * Opens a Log instance with the specified storage configuration.
     *
     * @param storageType   storage backend type
     * @param path          data path for SlateDB (ignored for in-memory)
     * @param objectStore   object store type: "in-memory", "local", or "s3"
     * @param s3Bucket      S3 bucket name (only for s3 object store)
     * @param s3Region      S3 region (only for s3 object store)
     * @param settingsPath  path to SlateDB settings file (optional)
     * @return a new Log instance
     */
    public static Log open(StorageType storageType, String path,
                           String objectStore, String s3Bucket, String s3Region,
                           String settingsPath) {
        long handle = nativeCreate(
                storageType.name(),
                path != null ? path : "/tmp/opendata",
                objectStore != null ? objectStore : "local",
                s3Bucket,
                s3Region,
                settingsPath);
        if (handle == 0) {
            throw new RuntimeException("Failed to create Log instance");
        }
        return new Log(handle);
    }

    /**
     * Appends a value to the log under the given key.
     *
     * <p>The native layer prepends a timestamp header to the value for latency
     * measurement. This is transparent to the caller.
     *
     * @param key   the key to append under
     * @param value the value to append
     * @return the result of the append operation, including the timestamp
     */
    public AppendResult append(byte[] key, byte[] value) {
        checkNotClosed();
        return nativeAppend(handle, key, value);
    }

    /**
     * Asynchronously appends a value to the log.
     *
     * @param key   the key to append under
     * @param value the value to append
     * @return a future that completes with the append result
     */
    public CompletableFuture<AppendResult> appendAsync(byte[] key, byte[] value) {
        // TODO: Implement async append
        return CompletableFuture.supplyAsync(() -> append(key, value));
    }

    /**
     * Creates a reader for this log.
     *
     * <p>The reader can be used to read from any key. The key and sequence
     * are specified per-read operation, not at reader creation time.
     *
     * @return a new LogReader instance
     */
    public LogReader reader() {
        checkNotClosed();
        return LogReader.create(handle);
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
            throw new IllegalStateException("Log is closed");
        }
    }

    long getHandle() {
        return handle;
    }

    // Native methods
    private static native long nativeCreate(
            String storageType,
            String path,
            String objectStore,
            String s3Bucket,
            String s3Region,
            String settingsPath);
    private static native AppendResult nativeAppend(long handle, byte[] key, byte[] value);
    private static native void nativeClose(long handle);
}
