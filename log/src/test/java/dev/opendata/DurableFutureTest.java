package dev.opendata;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Exercises the durable-ack {@link CompletableFuture} returned in
 * {@link AppendResult#durable()}.
 *
 * <p>In-memory storage is immediately durable, so the durable future for
 * any append completes almost as soon as the native subscription delivers
 * the watermark advance.
 */
class DurableFutureTest {

    private static final long AWAIT_MS = 5_000;

    @Test
    void shouldExposeStartAndEndSequenceImmediately() {
        try (LogDb log = LogDb.openInMemory()) {
            AppendResult result = log.tryAppend(new Record[]{
                    new Record("k".getBytes(StandardCharsets.UTF_8),
                            "v1".getBytes(StandardCharsets.UTF_8)),
                    new Record("k".getBytes(StandardCharsets.UTF_8),
                            "v2".getBytes(StandardCharsets.UTF_8)),
                    new Record("k".getBytes(StandardCharsets.UTF_8),
                            "v3".getBytes(StandardCharsets.UTF_8)),
            });

            assertThat(result.startSequence()).isEqualTo(0);
            assertThat(result.endSequence()).isEqualTo(3);
        }
    }

    @Test
    void shouldCompleteDurableFutureForInMemoryAppend() throws Exception {
        try (LogDb log = LogDb.openInMemory()) {
            AppendResult result = log.tryAppend(
                    "k".getBytes(StandardCharsets.UTF_8),
                    "v".getBytes(StandardCharsets.UTF_8));

            result.durable().get(AWAIT_MS, TimeUnit.MILLISECONDS);
            assertThat(result.durable().isDone()).isTrue();
            assertThat(result.durable().isCompletedExceptionally()).isFalse();
        }
    }

    @Test
    void shouldCompleteFuturesForEveryAppendInOrder() throws Exception {
        try (LogDb log = LogDb.openInMemory()) {
            int n = 50;
            java.util.List<CompletableFuture<Void>> futures = new java.util.ArrayList<>(n);
            for (int i = 0; i < n; i++) {
                AppendResult r = log.tryAppend(
                        "k".getBytes(StandardCharsets.UTF_8),
                        ("v" + i).getBytes(StandardCharsets.UTF_8));
                assertThat(r.startSequence()).isEqualTo(i);
                assertThat(r.endSequence()).isEqualTo(i + 1);
                futures.add(r.durable());
            }
            CompletableFuture.allOf(futures.toArray(CompletableFuture[]::new))
                    .get(AWAIT_MS, TimeUnit.MILLISECONDS);
            for (CompletableFuture<Void> f : futures) {
                assertThat(f.isCompletedExceptionally()).isFalse();
            }
        }
    }

    @Test
    void shouldLeaveDurableFutureDoneAfterClose() {
        // Regression check: close() must leave no future in limbo. In-memory
        // storage is immediately durable, so the future almost always
        // completes normally before close — we only assert that *some*
        // completion happened, not which path. Deterministic coverage of the
        // LogClosedException path would require a slow storage backend or a
        // test-only hook to delay subscription delivery.
        LogDb log = LogDb.openInMemory();
        AppendResult result;
        try {
            result = log.tryAppend(
                    "k".getBytes(StandardCharsets.UTF_8),
                    "v".getBytes(StandardCharsets.UTF_8));
        } finally {
            log.close();
        }
        assertThat(result.durable().isDone()).isTrue();
    }

    @Test
    void shouldCompleteDurableFutureForRecordBatchAppend() throws Exception {
        try (LogDb log = LogDb.openInMemory()) {
            byte[] key = "batch".getBytes(StandardCharsets.UTF_8);
            try (RecordBatch batch = RecordBatch.create()) {
                batch.add(key, "v0".getBytes(StandardCharsets.UTF_8), 1000L);
                batch.add(key, "v1".getBytes(StandardCharsets.UTF_8), 1001L);
                batch.add(key, "v2".getBytes(StandardCharsets.UTF_8), 1002L);

                AppendResult result = log.tryAppend(batch);

                assertThat(result.startSequence()).isEqualTo(0);
                assertThat(result.endSequence()).isEqualTo(3);
                result.durable().get(AWAIT_MS, TimeUnit.MILLISECONDS);
                assertThat(result.durable().isCompletedExceptionally()).isFalse();
            }
        }
    }

    @Test
    void shouldRejectAppendAfterClose() {
        LogDb log = LogDb.openInMemory();
        log.close();
        assertThatThrownBy(() -> log.tryAppend(
                "k".getBytes(StandardCharsets.UTF_8),
                "v".getBytes(StandardCharsets.UTF_8)))
                .isInstanceOf(IllegalStateException.class);
    }
}
