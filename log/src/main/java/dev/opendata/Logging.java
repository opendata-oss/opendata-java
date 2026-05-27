package dev.opendata;

/**
 * Installs the native log library's {@code tracing} subscriber.
 *
 * <p>Must be called at most once per process, before opening any
 * {@link LogDb} or {@link LogDbReader}. Hosts that already manage their own
 * {@code tracing} subscriber should not call this.
 */
public final class Logging {

    private Logging() {
    }

    /**
     * Installs the native tracing subscriber writing to stderr.
     *
     * @param filter an {@code EnvFilter} directive (same syntax as
     *               {@code RUST_LOG}), e.g. {@code "info"} or
     *               {@code "slatedb=debug"}. Pass {@code null} to fall back
     *               to {@code RUST_LOG} (or {@code "info"} if unset).
     * @throws dev.opendata.common.OpenDataNativeException if a subscriber is
     *         already installed in the process
     */
    public static void enable(String filter) {
        NativeInterop.enableLogging(filter);
    }
}
