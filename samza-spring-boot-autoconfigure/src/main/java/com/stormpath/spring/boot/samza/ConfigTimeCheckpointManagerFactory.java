package com.stormpath.spring.boot.samza;

import org.apache.samza.checkpoint.CheckpointManager;
import org.apache.samza.checkpoint.CheckpointManagerFactory;
import org.apache.samza.config.Config;
import org.apache.samza.metrics.MetricsRegistry;
import org.springframework.util.Assert;

public class ConfigTimeCheckpointManagerFactory implements CheckpointManagerFactory {

    private static CheckpointManagerFactory delegate;

    protected CheckpointManagerFactory getDelegate() {
        Assert.notNull(delegate, "Static delegate CheckpointManagerFactory cannot be null.");
        return delegate;
    }

    public static void setCheckpointManagerFactory(CheckpointManagerFactory factory) {
        ConfigTimeCheckpointManagerFactory.delegate = factory;
    }

    @Override
    public CheckpointManager getCheckpointManager(Config config, MetricsRegistry registry) {
        return getDelegate().getCheckpointManager(config, registry);
    }
}
