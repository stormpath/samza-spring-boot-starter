package com.stormpath.samza.curator.checkpoint;

import com.stormpath.samza.lang.Assert;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.api.transaction.CuratorTransaction;
import org.apache.curator.framework.api.transaction.CuratorTransactionFinal;
import org.apache.samza.SamzaException;
import org.apache.samza.checkpoint.Checkpoint;
import org.apache.samza.checkpoint.CheckpointManager;
import org.apache.samza.container.TaskName;
import org.apache.samza.serializers.CheckpointSerde;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.*;

public class ZookeeperCheckpointManager implements CheckpointManager {

    private static final Logger log = LoggerFactory.getLogger(ZookeeperCheckpointManager.class);

    private static final byte[] EMPTY_BYTES = new byte[0];
    private static final CheckpointSerde CHECKPOINT_SERDE = new CheckpointSerde();

    private final String tasksPath;
    private final Set<TaskName> registeredTaskNames = new HashSet<>();
    private final CuratorFramework curator;

    public ZookeeperCheckpointManager(CuratorFramework curator, String jobPath) {
        Assert.hasText(jobPath, "jobPath cannot be null or empty.");
        this.tasksPath = jobPath + "/tasks";
        this.curator = Assert.notNull(curator, "Curator cannot be null.");
    }

    @Override
    public void start() {
        log.debug("Starting {}...", getClass().getSimpleName());
        try {
            doStart();
            log.info("Started {}", getClass().getSimpleName());
        } catch (Exception e) {
            String msg = "Unable to start " + getClass().getSimpleName() + ": " + e.getMessage();
            throw new SamzaException(msg, e);
        }
    }

    private void doStart() throws Exception {

        //ensure base path node exists:
        try {
            String result = curator.create().creatingParentsIfNeeded().forPath(tasksPath, EMPTY_BYTES);
            log.info("Created tasks node {}", result);
        } catch (KeeperException.NodeExistsException e) {
            log.info("Tasks node already exists: {}", tasksPath);
            log.trace("Exception details: ", e);
        }

        //can be empty if start is being called for changelog partition number duty.  Samza does not
        //register task names until *after* changelog partition number registrations are reported
        // (at which point it will call stop, register task names and then call start again
        //  and this collection will be non-empty):
        if (registeredTaskNames.isEmpty()) {
            return;
        }

        log.info("Ensuring checkpoint nodes exist for registeredTaskNames: {}", registeredTaskNames);
        for (TaskName tn : registeredTaskNames) {

            String checkpointPath = getCheckpointPath(tn);
            try {
                String result = curator.create().creatingParentsIfNeeded().forPath(checkpointPath, EMPTY_BYTES);
                log.info("Created checkpoint node {}", result);
            } catch (KeeperException.NodeExistsException e) {
                log.info("Checkpoint node already exists: {}", checkpointPath);
                log.trace("Exception details: ", e);
            }
        }
    }

    @Override
    public void register(TaskName taskName) {
        registeredTaskNames.add(taskName);
        log.info("Registered TaskName '{}' for checkpoint management", taskName);
    }

    protected String getCheckpointPath(TaskName tn) {
        return tasksPath + "/" + tn.getTaskName() + "/" + "checkpoint";
    }

    protected String getChangelogPartitionNumberPath(TaskName tn) {
        return tasksPath + "/" + tn.getTaskName() + "/" + "changelogPartitionNumber";
    }

    @Override
    public void writeCheckpoint(TaskName taskName, Checkpoint checkpoint) {
        try {
            log.info("Writing Task {} {}", taskName, checkpoint);
            doWriteCheckpoint(taskName, checkpoint);
        } catch (Exception e) {
            String msg = "Unable to write checkpoint " + checkpoint + " for taskName " + taskName + ": " + e.getMessage();
            throw new SamzaException(msg, e);
        }
    }

    protected void doWriteCheckpoint(TaskName tn, Checkpoint checkpoint) throws Exception {
        String checkpointPath = getCheckpointPath(tn);
        byte[] serialized = CHECKPOINT_SERDE.toBytes(checkpoint);
        curator.setData().forPath(checkpointPath, serialized);
    }

    @Override
    public Checkpoint readLastCheckpoint(TaskName taskName) {
        try {
            Checkpoint checkpoint = doReadLastCheckpoint(taskName);
            log.info("Read last Checkpoint {} for TaskName {}", checkpoint, taskName);
            return checkpoint;
        } catch (Exception e) {
            String msg = "Unable to read last checkpoint for task name " + taskName + ": " + e.getMessage();
            throw new SamzaException(msg, e);
        }
    }

    protected Checkpoint doReadLastCheckpoint(TaskName tn) throws Exception {

        assertRegistered(tn);

        String checkpointPath = getCheckpointPath(tn);
        byte[] data = curator.getData().forPath(checkpointPath);
        if (data != null && data.length > 0) {
            return CHECKPOINT_SERDE.fromBytes(data);
        }
        return null;
    }

    protected static byte[] intToBytes(int i) {
        ByteBuffer bb = ByteBuffer.allocate(4);
        bb.putInt(i);
        return bb.array();
    }

    protected static int bytesToInt(byte[] bytes) {
        Assert.notNull(bytes, "bytes cannot be null.");
        Assert.isTrue(bytes.length == 4, "integer byte arrays must be 4 bytes long.");
        ByteBuffer bb = ByteBuffer.wrap(bytes);
        return bb.getInt();
    }

    protected void assertRegistered(TaskName tn) {
        if (!registeredTaskNames.contains(tn)) {
            throw new SamzaException(tn + " is not registered with this CheckpointManager.");
        }
    }

    @Override
    public Map<TaskName, Integer> readChangeLogPartitionMapping() {
        try {
            Map<TaskName, Integer> mapping = doReadChangeLogPartitionMapping();
            log.info("Read changelog partition mapping {}", mapping);
            return mapping;
        } catch (Exception e) {
            String msg = "Unable to read change log partition mapping: " + e.getMessage();
            throw new SamzaException(msg, e);
        }
    }

    protected Map<TaskName, Integer> doReadChangeLogPartitionMapping() throws Exception {

        Map<TaskName, Integer> m = new HashMap<>();

        List<String> children;
        try {
            children = curator.getChildren().forPath(tasksPath);
        } catch (KeeperException.NoNodeException nne) {
            //can be null if no tasks have been registered yet - i.e. very first run of a job
            return Collections.emptyMap();
        }

        log.debug("Getting changelog partition mappings for tasks {}", children);
        for (String taskName : children) {
            String path = tasksPath + "/" + taskName;
            byte[] data = curator.getData().forPath(path);
            if (data != null && data.length > 0) {
                int val = bytesToInt(data);
                TaskName tn = new TaskName(taskName);
                m.put(tn, val);
            }
        }

        return m;
    }

    @Override
    public void writeChangeLogPartitionMapping(Map<TaskName, Integer> mapping) {
        try {
            doWriteChangeLogPartitionMapping(mapping);
            log.info("Wrote changelog partition mapping {}", mapping);
        } catch (Exception e) {
            String msg = "Unable to write changelog partition mapping: " + e.getMessage();
            throw new SamzaException(msg, e);
        }
    }

    protected boolean createChangeLogPartitionPathIfNecessary(String clpnPath, byte[] data) throws Exception {
        try {
            String result = curator.create().creatingParentsIfNeeded().forPath(clpnPath, data);
            log.info("Created changelog partition number node {}", result);
            return true;
        } catch (KeeperException.NodeExistsException e) {
            log.info("Changelog partition number node already exists: {}", clpnPath);
            log.trace("Exception details: ", e);
            return false;
        }
    }

    protected void doWriteChangeLogPartitionMapping(Map<TaskName, Integer> mapping) throws Exception {

        CuratorTransaction transaction = curator.inTransaction();
        boolean needTransaction = false;

        for (Map.Entry<TaskName, Integer> entry : mapping.entrySet()) {

            Integer partitionNumber = entry.getValue();
            TaskName tn = entry.getKey();

            String clpnPath = getChangelogPartitionNumberPath(tn);
            byte[] data = intToBytes(partitionNumber);

            boolean created = createChangeLogPartitionPathIfNecessary(clpnPath, data);

            if (!created) {//create would have written with the data, but since we didn't create, we have to set it now:
                transaction.setData().forPath(clpnPath, data);
                needTransaction = true;
                log.debug("Appended changelog partition mapping {}={} to current transaction.", tn, partitionNumber);
            }
        }

        if (needTransaction) {
            ((CuratorTransactionFinal) transaction).commit();
        }

        log.info("Wrote changelog partition mappings {}", mapping);
    }

    @Override
    public void stop() {
        log.debug("Stopped {}", getClass().getSimpleName());
    }
}
