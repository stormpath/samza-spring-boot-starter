package com.stormpath.spring.boot.samza

import org.apache.samza.config.Config
import org.apache.samza.system.IncomingMessageEnvelope
import org.apache.samza.task.*
import org.slf4j.Logger
import org.slf4j.LoggerFactory

class TestTask implements StreamTask, InitableTask {

    private static final Logger log = LoggerFactory.getLogger(TestTask.class);

    @Override
    public void init(Config config, TaskContext taskContext) throws Exception {
        log.info("init config: {}", config);
        log.info("init taskContext: {}", taskContext);
    }

    @Override
    public void process(IncomingMessageEnvelope envelope, MessageCollector messageCollector, TaskCoordinator taskCoordinator) throws Exception {
        log.info("Received incoming message envelope: {}", envelope);
    }
}
