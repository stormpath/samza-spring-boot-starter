package com.stormpath.spring.boot.samza;

import org.apache.samza.checkpoint.CheckpointManager;
import org.apache.samza.checkpoint.CheckpointManagerFactory;
import org.apache.samza.config.Config;
import org.apache.samza.metrics.MetricsRegistry;
import org.springframework.util.Assert;

public class FixedCheckpointManagerFactory implements CheckpointManagerFactory {

    private final CheckpointManager checkpointManager;

    public FixedCheckpointManagerFactory(CheckpointManager checkpointManager) {
        Assert.notNull(checkpointManager, "CheckpointManager cannot be null.");
        this.checkpointManager = checkpointManager;
    }

    @Override
    public CheckpointManager getCheckpointManager(Config config, MetricsRegistry registry) {
        return checkpointManager;
    }
}
