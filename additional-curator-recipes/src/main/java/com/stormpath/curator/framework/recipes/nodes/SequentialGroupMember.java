package com.stormpath.curator.framework.recipes.nodes;

import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.nodes.PersistentNode;
import org.apache.curator.utils.CloseableUtils;
import org.apache.curator.utils.ThreadUtils;
import org.apache.curator.utils.ZKPaths;
import org.apache.zookeeper.CreateMode;
import org.springframework.util.Assert;

import java.io.Closeable;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class SequentialGroupMember implements Closeable {

    private final PersistentNode pen;
    private final PathChildrenCache cache;

    private int id = -1;

    /**
     * @param client         client
     * @param membershipPath the path to use for membership
     * @param payload        the payload to write in our member node
     */
    public SequentialGroupMember(CuratorFramework client, String membershipPath, byte[] payload) {
        cache = newPathChildrenCache(client, membershipPath);
        pen = new PersistentNode(client, CreateMode.EPHEMERAL_SEQUENTIAL, true, membershipPath, payload);
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
        pen.start();
        try {
            pen.waitForInitialCreate(30000, TimeUnit.SECONDS);
            this.id = idFromPath(pen.getActualPath());
            cache.start();
        } catch (Exception e) {
            ThreadUtils.checkInterrupted(e);
            Throwables.propagate(e);
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
        CloseableUtils.closeQuietly(cache);
        CloseableUtils.closeQuietly(pen);
    }

    /**
     * Return the current view of membership. The keys are the IDs
     * of the members. The values are each member's payload
     *
     * @return membership
     */
    public Map<Integer, byte[]> getCurrentMembers() {
        ImmutableMap.Builder<Integer, byte[]> builder = ImmutableMap.builder();
        boolean thisIdAdded = false;
        for (ChildData data : cache.getCurrentData()) {
            String path = data.getPath();
            int pathId = idFromPath(path);
            thisIdAdded = thisIdAdded || pathId == this.id;
            builder.put(pathId, data.getData());
        }
        if (!thisIdAdded) {
            builder.put(this.id, pen.getData());   // this instance is always a member
        }
        return builder.build();
    }

    /**
     * Given a full ZNode path, return the member ID
     *
     * @param path full ZNode path
     * @return id
     */
    public int idFromPath(String path) {
        String idVal = ZKPaths.getNodeFromPath(path);
        return Integer.parseInt(idVal);
    }

    protected PathChildrenCache newPathChildrenCache(CuratorFramework client, String membershipPath) {
        return new PathChildrenCache(client, membershipPath, true);
    }
}
