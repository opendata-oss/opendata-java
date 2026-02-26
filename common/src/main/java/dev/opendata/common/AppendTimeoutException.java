package dev.opendata.common;

/**
 * Thrown when an append with a timeout expires before the write queue has space.
 *
 * <p>The caller can retry the same batch of records.
 */
public class AppendTimeoutException extends OpenDataNativeException {

    public AppendTimeoutException(String message) {
        super(message);
    }
}
