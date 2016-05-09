package com.stormpath.spring.boot.samza.curator.checkpoint;

import com.stormpath.samza.curator.checkpoint.ZookeeperCheckpointManager;
import org.apache.curator.framework.CuratorFramework;
import org.apache.samza.checkpoint.CheckpointManager;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.AutoConfigureOrder;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
@ConditionalOnProperty(name = {"samza.enabled", "spring.cloud.zookeeper.enabled", "samza.zookeeper.enabled", "samza.zookeeper.checkpointManager.enabled"}, matchIfMissing = true)
@AutoConfigureOrder(10)
public class ZookeeperCheckpointAutoConfiguration {

    @Autowired
    private CuratorFramework curator;

    @Value("#{@environment['samza.zookeper.jobs.namespace'] ?: '/samza/jobs'}")
    private String jobsNamespace;

    @Value("#{ @environment['samza.job.name'] ?: (@environment['spring.application.name'] ?: null) }")
    private String samzaJobName;

    @Bean
    @ConditionalOnMissingBean(name = "samzaJobZookeeperPath")
    public String samzaJobZookeeperPath() {
        return jobsNamespace + "/" + samzaJobName;
    }

    @Bean
    //do not specify the init/destroy methods here - the SamzaContainer instance calls these methods when necessary
    @ConditionalOnMissingBean
    public CheckpointManager samzaCheckpointManager() {
        String jobPath = samzaJobZookeeperPath();
        return new ZookeeperCheckpointManager(curator, jobPath);
    }
}
