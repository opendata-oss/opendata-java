package dev.opendata;

/**
 * Controls how LSM compaction is scheduled for a {@link LogDb}.
 *
 * <ul>
 *   <li>{@link #DEFAULT} — uses SlateDB's built-in compaction scheduler.
 *   <li>{@link #L0_ONLY} — only compacts L0 SSTs, never merges sorted runs.
 * </ul>
 */
public enum CompactionMode {
    DEFAULT,
    L0_ONLY
}
