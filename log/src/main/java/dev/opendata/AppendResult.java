package dev.opendata;

import java.util.concurrent.CompletableFuture;

/**
 * Result of an append operation to the log.
 *
 * <p>{@link #startSequence()} and {@link #endSequence()} are assigned at submit
 * time and available immediately. {@link #durable()} returns a future that
 * completes when the records have been confirmed durable by the underlying
 * storage. The future completes exceptionally with
 * {@link dev.opendata.common.LogClosedException} if the log is closed before
 * the records become durable.
 *
 * <p>Sequence range is half-open: the appended batch occupies
 * {@code [startSequence, endSequence)}.
 *
 * <p>The future is completed on the native callback thread. Continuations
 * registered with non-async methods (e.g. {@code thenRun}) will run on that
 * thread, and <strong>must not call back into the same {@link LogDb}</strong>
 * — re-entry from the native callback thread deadlocks the runtime. Callers
 * that need to do non-trivial work or call other log methods in the
 * continuation must use the {@code *Async} variants.
 *
 * @param startSequence first sequence number assigned to this batch (inclusive)
 * @param endSequence   one past the last sequence number assigned (exclusive)
 * @param timestamp     timestamp (epoch millis) of the first record in the batch
 * @param durable       future that completes when the records are durable
 */
public record AppendResult(
        long startSequence,
        long endSequence,
        long timestamp,
        CompletableFuture<Void> durable
) {
}
