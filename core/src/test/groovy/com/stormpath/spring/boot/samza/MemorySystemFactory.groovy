package com.stormpath.spring.boot.samza

import org.apache.samza.config.Config
import org.apache.samza.metrics.MetricsRegistry
import org.apache.samza.system.SystemAdmin
import org.apache.samza.system.SystemConsumer
import org.apache.samza.system.SystemFactory
import org.apache.samza.system.SystemProducer
import org.apache.samza.util.SinglePartitionWithoutOffsetsSystemAdmin

/**
 * A Samza SystemFactory implementation that just uses memory - for testing only.
 */
class MemorySystemFactory implements SystemFactory {

    private MemorySystemConsumer consumer;
    private MemorySystemProducer producer;

    public MemorySystemFactory() {
        this.producer = new MemorySystemProducer();
        this.consumer = new MemorySystemConsumer(this.producer);
    }

    @Override
    SystemConsumer getConsumer(String systemName, Config config, MetricsRegistry registry) {
        return this.consumer;
    }

    @Override
    SystemProducer getProducer(String systemName, Config config, MetricsRegistry registry) {
        return this.producer
    }

    @Override
    SystemAdmin getAdmin(String systemName, Config config) {
        return new SinglePartitionWithoutOffsetsSystemAdmin();
    }
}
