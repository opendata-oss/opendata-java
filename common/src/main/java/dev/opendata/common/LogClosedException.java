package dev.opendata.common;

/**
 * Signaled on a durable-ack future when the log was closed before
 * the corresponding append became durable.
 */
public class LogClosedException extends OpenDataNativeException {
    public LogClosedException() {
        super("log was closed before append became durable");
    }
}
