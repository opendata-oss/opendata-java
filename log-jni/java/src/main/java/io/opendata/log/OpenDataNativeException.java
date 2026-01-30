package io.opendata.log;

/**
 * Exception thrown by the native OpenData library.
 *
 * <p>This exception wraps errors that originate from the Rust native code,
 * including storage errors, I/O errors, and internal library errors.
 */
public class OpenDataNativeException extends RuntimeException {

    public OpenDataNativeException(String message) {
        super(message);
    }

    public OpenDataNativeException(String message, Throwable cause) {
        super(message, cause);
    }
}
