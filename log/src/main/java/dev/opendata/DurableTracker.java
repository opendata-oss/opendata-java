package dev.opendata;

import dev.opendata.common.LogClosedException;
import dev.opendata.ffi.opendata_log_subscribe_durable$callback;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Tracks pending appends against the native durable-sequence watermark.
 *
 * <p>Owns a native subscription that fires on each watermark advance. Each
 * append registers an {@code endSequence} (exclusive) with a future; when
 * the watermark reaches or passes that sequence the future is completed.
 *
 * <p>Lifecycle: closed strictly after the owning native log is closed (the
 * tokio task driving the callbacks exits when the log's watch channel drops),
 * then this class unsubscribes, drains any in-flight callback, completes
 * outstanding futures with {@link LogClosedException}, and frees the upcall
 * arena.
 */
final class DurableTracker implements AutoCloseable {

    private final ConcurrentSkipListMap<Long, CompletableFuture<Void>> pending =
            new ConcurrentSkipListMap<>();
    private final AtomicLong watermark = new AtomicLong(0);
    private final AtomicBoolean closed = new AtomicBoolean(false);
    private final AtomicInteger inFlight = new AtomicInteger(0);

    private final Arena upcallArena;
    private final NativeInterop.SubscriptionHandle subscription;

    DurableTracker(NativeInterop.LogHandle log) {
        this.upcallArena = Arena.ofShared();
        try {
            MemorySegment stub = opendata_log_subscribe_durable$callback.allocate(
                    this::onDurable, upcallArena);
            this.subscription = NativeInterop.subscribeDurable(log, stub);
        } catch (Throwable t) {
            upcallArena.close();
            throw t;
        }
    }

    /**
     * Registers a future to be completed when {@code endSequence <= watermark}.
     * Returns an already-completed future if the watermark has already passed.
     * Returns an already-failed future if the tracker is closed.
     */
    CompletableFuture<Void> register(long endSequence) {
        CompletableFuture<Void> future = new CompletableFuture<>();
        if (closed.get()) {
            future.completeExceptionally(new LogClosedException());
            return future;
        }
        if (watermark.get() >= endSequence) {
            future.complete(null);
            return future;
        }
        pending.put(endSequence, future);
        // Re-check after put: handle races where the watermark advanced
        // or close started between the initial check and the put.
        if (closed.get()) {
            CompletableFuture<Void> removed = pending.remove(endSequence);
            if (removed != null) {
                removed.completeExceptionally(new LogClosedException());
            }
        } else if (watermark.get() >= endSequence) {
            CompletableFuture<Void> removed = pending.remove(endSequence);
            if (removed != null) {
                removed.complete(null);
            }
        }
        return future;
    }

    /**
     * Upcall target — invoked on a tokio worker thread by the native subscription.
     *
     * <p>The {@code userData} parameter is unused; the Java method reference
     * passed to the upcall stub already carries the {@code this} reference.
     */
    private void onDurable(long durableSequence, MemorySegment userData) {
        inFlight.incrementAndGet();
        try {
            if (closed.get()) {
                return;
            }
            watermark.updateAndGet(curr -> Math.max(curr, durableSequence));
            Map.Entry<Long, CompletableFuture<Void>> entry;
            while ((entry = pending.firstEntry()) != null
                    && entry.getKey() <= durableSequence) {
                CompletableFuture<Void> removed = pending.remove(entry.getKey());
                if (removed != null) {
                    removed.complete(null);
                }
            }
        } finally {
            inFlight.decrementAndGet();
        }
    }

    @Override
    public void close() {
        if (!closed.compareAndSet(false, true)) {
            return;
        }
        // Best-effort cancel — the underlying tokio task has likely already
        // exited because the caller closed the native log first.
        subscription.close();
        // Wait for any in-flight callback to drain so the arena (and its
        // upcall stub) outlives every native invocation.
        while (inFlight.get() > 0) {
            Thread.onSpinWait();
        }
        LogClosedException ex = new LogClosedException();
        Map.Entry<Long, CompletableFuture<Void>> entry;
        while ((entry = pending.pollFirstEntry()) != null) {
            entry.getValue().completeExceptionally(ex);
        }
        upcallArena.close();
    }
}
