package dev.opendata;

/**
 * Storage backend type for the LogDb.
 */
public enum StorageType {
    /** In-memory storage (fast, no persistence). */
    IN_MEMORY,
    /** SlateDB-backed storage (persistent). */
    SLATEDB
}
