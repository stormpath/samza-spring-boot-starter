package com.stormpath.spring.boot.samza;

import org.apache.samza.checkpoint.Checkpoint;
import org.apache.samza.checkpoint.CheckpointManager;
import org.apache.samza.container.TaskName;

import java.util.Map;

public class DisabledCheckpointManager implements CheckpointManager {

    @Override
    public void start() {
    }

    @Override
    public void register(TaskName taskName) {
    }

    @Override
    public void writeCheckpoint(TaskName taskName, Checkpoint checkpoint) {
    }

    @Override
    public Checkpoint readLastCheckpoint(TaskName taskName) {
        return null;
    }

    @Override
    public Map<TaskName, Integer> readChangeLogPartitionMapping() {
        return null;
    }

    @Override
    public void writeChangeLogPartitionMapping(Map<TaskName, Integer> mapping) {
    }

    @Override
    public void stop() {
    }
}
