package com.stormpath.spring.boot.samza.curator.checkpoint;

import com.stormpath.samza.curator.checkpoint.ZookeeperCheckpointManager;
import org.apache.curator.framework.CuratorFramework;
import org.apache.samza.checkpoint.CheckpointManager;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
@ConditionalOnProperty(name = {"samza.enabled", "spring.cloud.zookeeper.enabled", "samza.zookeeper.enabled", "samza.zookeeper.checkpointManager.enabled"}, matchIfMissing = true)
public class ZookeeperCheckpointAutoConfiguration {

    @Autowired
    private CuratorFramework curator;

    @Value("#{@environment['samza.zookeper.jobs.namespace'] ?: '/samza/jobs'}")
    private String jobsNamespace;

    @SuppressWarnings("SpringJavaAutowiringInspection")
    @Autowired
    @Qualifier("samzaJobName")
    private String samzaJobName; //defined in SamzaAutoConfiguration

    @Bean
    @ConditionalOnMissingBean(name="samzaJobZookeeperPath")
    public String samzaJobZookeeperPath() {
        return jobsNamespace + "/" + samzaJobName;
    }

    @Bean //do not specify the init/destroy methods here - the SamzaContainer instance calls these methods when necessary
    @ConditionalOnMissingBean
    public CheckpointManager samzaCheckpointManager() {
        String jobPath = samzaJobZookeeperPath();
        return new ZookeeperCheckpointManager(curator, jobPath);
    }
}
