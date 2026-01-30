package io.opendata.benchmark;

import io.openmessaging.benchmark.driver.BenchmarkConsumer;
import io.openmessaging.benchmark.driver.BenchmarkDriver;
import io.openmessaging.benchmark.driver.BenchmarkProducer;
import io.openmessaging.benchmark.driver.ConsumerCallback;
import io.opendata.log.Log;

import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.bookkeeper.stats.StatsLogger;

/**
 * OpenMessaging Benchmark driver for OpenData Log.
 *
 * <p>Maps OMB concepts to Log operations:
 * <ul>
 *   <li>Topic + Partitions → Log partition-keys: "{topic}/0", "{topic}/1", ...</li>
 *   <li>Producer → Log.append() with key routing</li>
 *   <li>Consumer → LogReader (polling-based initially)</li>
 * </ul>
 *
 * <p>Configuration is loaded from a YAML file. See {@link OpendataConfig} for options.
 */
public class OpendataDriver implements BenchmarkDriver {

    private Log log;
    private OpendataConfig config;

    /** Tracks partition count per topic for producer/consumer creation. */
    private final Map<String, Integer> topicPartitions = new ConcurrentHashMap<>();

    @Override
    public void initialize(File configurationFile, StatsLogger statsLogger) throws IOException {
        this.config = OpendataConfig.load(configurationFile);

        // Map config storage type to Log.StorageType
        Log.StorageType storageType = "slatedb".equalsIgnoreCase(config.storage.type)
                ? Log.StorageType.SLATEDB
                : Log.StorageType.IN_MEMORY;

        this.log = Log.open(
                storageType,
                config.storage.path,
                config.storage.objectStore,
                config.storage.s3Bucket,
                config.storage.s3Region,
                config.storage.settingsPath);
    }

    @Override
    public String getTopicNamePrefix() {
        return "opendata-";
    }

    @Override
    public CompletableFuture<Void> createTopic(String topic, int partitions) {
        // Log doesn't require explicit topic/key creation
        // Keys are created implicitly on first append
        // Track partition count for later producer/consumer creation
        topicPartitions.put(topic, partitions);
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletableFuture<BenchmarkProducer> createProducer(String topic) {
        int partitions = topicPartitions.getOrDefault(topic, 1);
        BenchmarkProducer producer = new OpendataProducer(log, topic, partitions);
        return CompletableFuture.completedFuture(producer);
    }

    @Override
    public CompletableFuture<BenchmarkConsumer> createConsumer(
            String topic,
            String subscriptionName,
            ConsumerCallback callback) {
        int partitions = topicPartitions.getOrDefault(topic, 1);

        // Consumer reads from all partitions for this topic
        BenchmarkConsumer consumer = new OpendataConsumer(
                log,
                topic,
                partitions,
                config.consumer,
                callback);
        return CompletableFuture.completedFuture(consumer);
    }

    @Override
    public void close() throws Exception {
        if (log != null) {
            log.close();
        }
    }
}
