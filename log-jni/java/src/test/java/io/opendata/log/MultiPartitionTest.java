package io.opendata.log;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration tests for multi-partition produce/consume patterns.
 *
 * <p>These tests simulate the OMB driver's partition-based routing without
 * depending on the OMB framework.
 */
class MultiPartitionTest {

    private Log log;

    @BeforeEach
    void setUp() {
        log = Log.open("unused");
    }

    @AfterEach
    void tearDown() {
        if (log != null) {
            log.close();
        }
    }

    @Test
    void should_write_to_multiple_partitions_with_round_robin() {
        // given
        String topic = "round-robin-test";
        int numPartitions = 3;
        int messagesPerPartition = 10;
        byte[][] partitionKeys = createPartitionKeys(topic, numPartitions);

        // when - write messages round-robin across partitions
        for (int i = 0; i < numPartitions * messagesPerPartition; i++) {
            int partition = i % numPartitions;
            byte[] value = ("message-" + i).getBytes(StandardCharsets.UTF_8);
            log.append(partitionKeys[partition], value);
        }

        // then - each partition should have exactly messagesPerPartition entries
        LogReader reader = log.reader();
        for (int p = 0; p < numPartitions; p++) {
            List<LogEntry> entries = reader.read(partitionKeys[p], 0, 100);
            assertThat(entries).hasSize(messagesPerPartition);
        }
        reader.close();
    }

    @Test
    void should_write_to_partitions_based_on_key_hash() {
        // given
        String topic = "hash-routing-test";
        int numPartitions = 4;
        byte[][] partitionKeys = createPartitionKeys(topic, numPartitions);
        String[] messageKeys = {"user-1", "user-2", "user-3", "user-1", "user-2", "user-1"};

        // when - write messages with keys hashed to partitions
        for (String messageKey : messageKeys) {
            int partition = Math.abs(messageKey.hashCode()) % numPartitions;
            byte[] value = ("value-for-" + messageKey).getBytes(StandardCharsets.UTF_8);
            log.append(partitionKeys[partition], value);
        }

        // then - same keys should go to same partitions
        LogReader reader = log.reader();
        int user1Partition = Math.abs("user-1".hashCode()) % numPartitions;
        List<LogEntry> user1Entries = reader.read(partitionKeys[user1Partition], 0, 100);

        // user-1 appears 3 times
        assertThat(user1Entries).hasSize(3);
        reader.close();
    }

    @Test
    void should_consume_from_multiple_partitions_concurrently() throws Exception {
        // given
        String topic = "concurrent-consume-test";
        int numPartitions = 3;
        int messagesPerPartition = 100;
        byte[][] partitionKeys = createPartitionKeys(topic, numPartitions);

        // Write messages to all partitions
        for (int p = 0; p < numPartitions; p++) {
            for (int i = 0; i < messagesPerPartition; i++) {
                byte[] value = ("p" + p + "-msg-" + i).getBytes(StandardCharsets.UTF_8);
                log.append(partitionKeys[p], value);
            }
        }

        // when - consume from all partitions concurrently using shared queue pattern
        BlockingQueue<LogEntry> entryQueue = new LinkedBlockingQueue<>();
        AtomicBoolean running = new AtomicBoolean(true);
        LogReader reader = log.reader();

        ExecutorService pollers = Executors.newFixedThreadPool(numPartitions);
        for (int p = 0; p < numPartitions; p++) {
            final byte[] partitionKey = partitionKeys[p];
            pollers.submit(() -> {
                long seq = 0;
                while (running.get()) {
                    List<LogEntry> entries = reader.read(partitionKey, seq, 50);
                    for (LogEntry entry : entries) {
                        entryQueue.offer(entry);
                        seq = entry.sequence() + 1;
                    }
                    if (entries.isEmpty()) {
                        try {
                            Thread.sleep(5);
                        } catch (InterruptedException e) {
                            break;
                        }
                    }
                }
            });
        }

        // Collect entries until we have all of them
        List<LogEntry> collected = new ArrayList<>();
        int expectedTotal = numPartitions * messagesPerPartition;
        long deadline = System.currentTimeMillis() + 5000;

        while (collected.size() < expectedTotal && System.currentTimeMillis() < deadline) {
            LogEntry entry = entryQueue.poll(100, TimeUnit.MILLISECONDS);
            if (entry != null) {
                collected.add(entry);
            }
        }

        running.set(false);
        pollers.shutdownNow();
        reader.close();

        // then
        assertThat(collected).hasSize(expectedTotal);

        // Verify we got messages from all partitions
        Map<String, Integer> partitionCounts = new ConcurrentHashMap<>();
        for (LogEntry entry : collected) {
            String key = new String(entry.key(), StandardCharsets.UTF_8);
            partitionCounts.merge(key, 1, Integer::sum);
        }
        assertThat(partitionCounts).hasSize(numPartitions);
        for (int count : partitionCounts.values()) {
            assertThat(count).isEqualTo(messagesPerPartition);
        }
    }

    @Test
    void should_maintain_order_within_each_partition() throws Exception {
        // given
        String topic = "order-test";
        int numPartitions = 2;
        int messagesPerPartition = 50;
        byte[][] partitionKeys = createPartitionKeys(topic, numPartitions);

        // Write messages with sequence in value
        for (int p = 0; p < numPartitions; p++) {
            for (int i = 0; i < messagesPerPartition; i++) {
                byte[] value = String.valueOf(i).getBytes(StandardCharsets.UTF_8);
                log.append(partitionKeys[p], value);
            }
        }

        // when - read from each partition
        LogReader reader = log.reader();

        // then - messages within each partition should be in order
        for (int p = 0; p < numPartitions; p++) {
            List<LogEntry> entries = reader.read(partitionKeys[p], 0, 100);
            assertThat(entries).hasSize(messagesPerPartition);

            for (int i = 0; i < entries.size(); i++) {
                String value = new String(entries.get(i).value(), StandardCharsets.UTF_8);
                assertThat(Integer.parseInt(value)).isEqualTo(i);
            }
        }
        reader.close();
    }

    @Test
    void should_handle_concurrent_produce_and_consume() throws Exception {
        // given
        String topic = "concurrent-produce-consume";
        int numPartitions = 2;
        int totalMessages = 200;
        byte[][] partitionKeys = createPartitionKeys(topic, numPartitions);

        BlockingQueue<LogEntry> consumed = new LinkedBlockingQueue<>();
        AtomicBoolean producerDone = new AtomicBoolean(false);
        AtomicBoolean consumerRunning = new AtomicBoolean(true);
        AtomicLong[] sequences = new AtomicLong[numPartitions];
        for (int i = 0; i < numPartitions; i++) {
            sequences[i] = new AtomicLong(0);
        }

        LogReader reader = log.reader();
        ExecutorService executor = Executors.newFixedThreadPool(numPartitions + 1);

        // Start consumers first
        for (int p = 0; p < numPartitions; p++) {
            final int partition = p;
            final byte[] partitionKey = partitionKeys[p];
            executor.submit(() -> {
                while (consumerRunning.get() || !producerDone.get()) {
                    List<LogEntry> entries = reader.read(partitionKey, sequences[partition].get(), 50);
                    for (LogEntry entry : entries) {
                        consumed.offer(entry);
                        sequences[partition].set(entry.sequence() + 1);
                    }
                    if (entries.isEmpty()) {
                        try {
                            Thread.sleep(5);
                        } catch (InterruptedException e) {
                            break;
                        }
                    }
                }
            });
        }

        // when - produce messages concurrently
        CountDownLatch producerLatch = new CountDownLatch(1);
        executor.submit(() -> {
            for (int i = 0; i < totalMessages; i++) {
                int partition = i % numPartitions;
                byte[] value = ("msg-" + i).getBytes(StandardCharsets.UTF_8);
                log.append(partitionKeys[partition], value);
            }
            producerDone.set(true);
            producerLatch.countDown();
        });

        // Wait for producer to finish
        producerLatch.await(5, TimeUnit.SECONDS);

        // Wait for consumers to catch up
        long deadline = System.currentTimeMillis() + 5000;
        while (consumed.size() < totalMessages && System.currentTimeMillis() < deadline) {
            Thread.sleep(50);
        }

        consumerRunning.set(false);
        executor.shutdownNow();
        reader.close();

        // then
        assertThat(consumed).hasSize(totalMessages);
    }

    @Test
    void should_resume_consumption_from_last_sequence() {
        // given
        String topic = "resume-test";
        byte[] partitionKey = (topic + "/0").getBytes(StandardCharsets.UTF_8);

        // Write initial batch
        for (int i = 0; i < 10; i++) {
            log.append(partitionKey, ("batch1-" + i).getBytes(StandardCharsets.UTF_8));
        }

        // Read and track last sequence
        LogReader reader = log.reader();
        List<LogEntry> batch1 = reader.read(partitionKey, 0, 100);
        assertThat(batch1).hasSize(10);
        long lastSeq = batch1.get(batch1.size() - 1).sequence();

        // Write more messages
        for (int i = 0; i < 5; i++) {
            log.append(partitionKey, ("batch2-" + i).getBytes(StandardCharsets.UTF_8));
        }

        // when - resume from last sequence + 1
        List<LogEntry> batch2 = reader.read(partitionKey, lastSeq + 1, 100);
        reader.close();

        // then - should only get new messages
        assertThat(batch2).hasSize(5);
        assertThat(new String(batch2.get(0).value(), StandardCharsets.UTF_8)).isEqualTo("batch2-0");
    }

    @Test
    void should_handle_empty_partitions() {
        // given
        String topic = "empty-partitions";
        int numPartitions = 3;
        byte[][] partitionKeys = createPartitionKeys(topic, numPartitions);

        // Only write to partition 1
        log.append(partitionKeys[1], "only-message".getBytes(StandardCharsets.UTF_8));

        // when
        LogReader reader = log.reader();
        List<LogEntry> entries0 = reader.read(partitionKeys[0], 0, 100);
        List<LogEntry> entries1 = reader.read(partitionKeys[1], 0, 100);
        List<LogEntry> entries2 = reader.read(partitionKeys[2], 0, 100);
        reader.close();

        // then
        assertThat(entries0).isEmpty();
        assertThat(entries1).hasSize(1);
        assertThat(entries2).isEmpty();
    }

    @Test
    void should_preserve_timestamps_across_partitions() throws Exception {
        // given
        String topic = "timestamp-test";
        int numPartitions = 2;
        byte[][] partitionKeys = createPartitionKeys(topic, numPartitions);

        long beforeWrite = System.currentTimeMillis();
        Thread.sleep(10); // Ensure some time passes

        // Write to both partitions
        AppendResult result0 = log.append(partitionKeys[0], "msg0".getBytes());
        AppendResult result1 = log.append(partitionKeys[1], "msg1".getBytes());

        Thread.sleep(10);
        long afterWrite = System.currentTimeMillis();

        // when
        LogReader reader = log.reader();
        List<LogEntry> entries0 = reader.read(partitionKeys[0], 0, 100);
        List<LogEntry> entries1 = reader.read(partitionKeys[1], 0, 100);
        reader.close();

        // then - timestamps should be within the write window
        assertThat(entries0.get(0).timestamp())
            .isGreaterThan(beforeWrite)
            .isLessThan(afterWrite)
            .isEqualTo(result0.timestamp());

        assertThat(entries1.get(0).timestamp())
            .isGreaterThan(beforeWrite)
            .isLessThan(afterWrite)
            .isEqualTo(result1.timestamp());
    }

    private byte[][] createPartitionKeys(String topic, int numPartitions) {
        byte[][] keys = new byte[numPartitions][];
        for (int i = 0; i < numPartitions; i++) {
            keys[i] = (topic + "/" + i).getBytes(StandardCharsets.UTF_8);
        }
        return keys;
    }
}
