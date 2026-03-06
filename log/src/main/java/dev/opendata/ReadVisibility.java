package dev.opendata;

/**
 * Controls which data is visible to reads on a {@link LogDb}.
 *
 * <ul>
 *   <li>{@link #MEMORY} — reads include in-memory (uncommitted) data.
 *   <li>{@link #REMOTE} — reads only see data confirmed durable by the storage engine.
 * </ul>
 */
public enum ReadVisibility {
    MEMORY,
    REMOTE
}
