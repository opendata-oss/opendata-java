package dev.opendata;

/**
 * Process-wide telemetry hooks for the native log library.
 *
 * <p>Installs a global Prometheus metrics-rs recorder that captures SlateDB
 * metrics emitted from any {@link LogDb} or {@link LogDbReader} opened in
 * this process, and renders them on demand in Prometheus text-exposition
 * format.
 *
 * <p>All methods are safe to call from any thread.
 */
public final class Telemetry {

    private Telemetry() {
    }

    /**
     * Installs the process-global Prometheus recorder. Idempotent: only the
     * first call installs the recorder; later calls observe the cached
     * result.
     *
     * @throws dev.opendata.common.OpenDataNativeException if a foreign
     *         metrics-rs recorder is already installed in the process
     */
    public static void init() {
        NativeInterop.initTelemetry();
    }

    /**
     * Renders the current SlateDB metrics as Prometheus text-exposition
     * output. Lazily installs the recorder if {@link #init()} was not called
     * explicitly. Returns an empty string if a foreign metrics-rs recorder
     * occupies the global slot.
     *
     * @return the metrics in Prometheus text format
     */
    public static String renderMetrics() {
        return NativeInterop.renderMetrics();
    }
}
