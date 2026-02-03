package dev.opendata;

/**
 * Configuration for log segmentation.
 *
 * <p>Segments partition the log into time-based chunks, enabling efficient
 * range queries and retention management.
 *
 * @param sealIntervalMs interval in milliseconds for automatic segment sealing,
 *                       or null to disable automatic sealing
 */
public record SegmentConfig(Long sealIntervalMs) {

    /**
     * Default configuration with automatic sealing disabled.
     */
    public static final SegmentConfig DEFAULT = new SegmentConfig(null);

    /**
     * Creates a segment config with automatic sealing at the specified interval.
     *
     * @param intervalMs interval in milliseconds between segment seals
     * @return a new SegmentConfig
     */
    public static SegmentConfig withSealInterval(long intervalMs) {
        if (intervalMs <= 0) {
            throw new IllegalArgumentException("sealIntervalMs must be positive");
        }
        return new SegmentConfig(intervalMs);
    }
}
