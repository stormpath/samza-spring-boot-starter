package com.stormpath.curator.framework.recipes.nodes;

import com.google.common.base.Throwables;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import org.apache.curator.framework.recipes.nodes.PersistentNode;
import org.apache.curator.utils.CloseableUtils;
import org.apache.curator.utils.ThreadUtils;
import org.apache.curator.utils.ZKPaths;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.Assert;

import java.io.Closeable;
import java.util.concurrent.TimeUnit;

public class SequentialGroupMember implements Closeable {

    private static final Logger log = LoggerFactory.getLogger(SequentialGroupMember.class);

    private final CuratorFramework client;
    private final int startingId;
    private final String membersBasePath;
    private final String locksBasePath;
    private final byte[] payload;

    private PersistentNode pen;
    private int id = -1;

    /**
     * @param client          client
     * @param membersBasePath the path to use for membership
     * @param payload         the payload to write in our member node
     */
    public SequentialGroupMember(CuratorFramework client, String membersBasePath, String locksBasePath, byte[] payload) {
        this(client, membersBasePath, locksBasePath, 0, payload);
    }

    /**
     * @param client          client
     * @param membersBasePath the path to use for membership
     * @param payload         the payload to write in our member node
     */
    public SequentialGroupMember(CuratorFramework client, String membersBasePath, String locksBasePath, int startingId, byte[] payload) {
        Assert.isTrue(startingId >= 0, "startingId must be greater than or equal to zero.");
        this.startingId = startingId;
        this.membersBasePath = membersBasePath;
        this.locksBasePath = locksBasePath;
        this.client = client;
        this.payload = payload;
    }

    public int getId() {
        Assert.isTrue(this.id != -1, "member id not yet acquired.  call start() first.");
        return this.id;
    }

    /**
     * Start the group membership. Register thisId as a member and begin
     * caching all members
     */
    public void start() {
        try {
            doStart();
        } catch (Exception e) {
            ThreadUtils.checkInterrupted(e);
            Throwables.propagate(e);
        }
    }

    protected void doStart() throws Exception {

        int i = startingId;

        while (this.id == -1) {

            String lockPath = ZKPaths.makePath(locksBasePath, String.valueOf(i));
            String memberPath = ZKPaths.makePath(membersBasePath, String.valueOf(i));

            log.trace("Acquiring mutex for member {} via lock path {}", i, lockPath);

            InterProcessMutex mutex = new InterProcessMutex(this.client, lockPath);
            mutex.acquire();

            log.debug("Acquired mutex for member {} via lock path {}", i, lockPath);

            try {

                Stat stat = client.checkExists().creatingParentContainersIfNeeded().forPath(memberPath);

                if (stat == null) {

                    log.debug("Claiming container id {} via member path {}", i, memberPath);

                    try {
                        //no peer has this node yet, grab it:
                        pen = new PersistentNode(client, CreateMode.EPHEMERAL, false, memberPath, payload);
                        pen.start();
                        pen.waitForInitialCreate(30000, TimeUnit.SECONDS);
                        this.id = i;
                        log.info("Claimed container id {} via member path {}", i, memberPath);
                        return;
                    } catch (InterruptedException e) {
                        CloseableUtils.closeQuietly(pen);
                        ThreadUtils.checkInterrupted(e);
                        Throwables.propagate(e);
                    }
                }

            } finally {
                mutex.release();
                log.debug("Released mutex for member {} via lock path {}", i, lockPath);
            }

            i++;
        }
    }

    /**
     * Change the data stored in this instance's node
     *
     * @param data new data (cannot be null)
     */
    public void setData(byte[] data) {
        try {
            pen.setData(data);
        } catch (Exception e) {
            ThreadUtils.checkInterrupted(e);
            Throwables.propagate(e);
        }
    }

    /**
     * Have thisId leave the group and stop caching membership
     */
    @Override
    public void close() {
        CloseableUtils.closeQuietly(pen);
    }
}
