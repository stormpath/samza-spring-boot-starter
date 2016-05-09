package com.stormpath.spring.boot.samza;

import org.apache.samza.checkpoint.CheckpointManager;
import org.apache.samza.checkpoint.CheckpointManagerFactory;
import org.apache.samza.config.Config;
import org.apache.samza.metrics.MetricsRegistry;
import org.springframework.util.Assert;

public class ConfigTimeCheckpointManagerFactory implements CheckpointManagerFactory {

    private CheckpointManagerFactory delegate;

    protected CheckpointManagerFactory getDelegate() {
        if (this.delegate == null) {
            this.delegate = SamzaAutoConfiguration.getConfigTimeCheckpointManagerFactory();
        }
        Assert.notNull(this.delegate, "Delegate CheckpointManagerFactory cannot be null.");
        return delegate;
    }

    @Override
    public CheckpointManager getCheckpointManager(Config config, MetricsRegistry registry) {
        return getDelegate().getCheckpointManager(config, registry);
    }
}
