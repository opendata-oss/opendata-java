package dev.opendata;

import dev.opendata.common.AppendTimeoutException;
import dev.opendata.common.OpenDataNativeException;
import dev.opendata.common.QueueFullException;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class NativeInteropTest {

    @Test
    void shouldMapQueueFullError() {
        RuntimeException ex = NativeInterop.mapError(5, "queue is full");
        assertThat(ex).isInstanceOf(QueueFullException.class)
                .hasMessage("queue is full");
    }

    @Test
    void shouldMapAppendTimeoutError() {
        RuntimeException ex = NativeInterop.mapError(6, "timed out");
        assertThat(ex).isInstanceOf(AppendTimeoutException.class)
                .hasMessage("timed out");
    }

    @Test
    void shouldMapUnknownErrorToNativeException() {
        RuntimeException ex = NativeInterop.mapError(99, "something broke");
        assertThat(ex).isInstanceOf(OpenDataNativeException.class)
                .hasMessage("something broke");
    }
}
