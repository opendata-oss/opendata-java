package dev.opendata.common;

/**
 * Thrown when a non-blocking append fails because the write queue is full.
 *
 * <p>The caller can retry the same batch of records after the queue drains.
 */
public class QueueFullException extends OpenDataNativeException {

    public QueueFullException(String message) {
        super(message);
    }
}
