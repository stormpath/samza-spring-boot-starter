package com.stormpath.spring.boot.samza.curator;

import com.stormpath.curator.framework.recipes.nodes.SequentialGroupMember;
import org.apache.curator.framework.CuratorFramework;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.AutoConfigureOrder;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.util.Assert;

@Configuration
@ConditionalOnProperty(name = {"samza.enabled", "spring.cloud.zookeeper.enabled", "samza.zookeeper.enabled"}, matchIfMissing = true)
@AutoConfigureOrder(1)
public class CuratorSamzaAutoConfiguration {

    private static final byte[] EMPTY_PAYLOAD = new byte[0];

    @SuppressWarnings("SpringJavaAutowiringInspection")
    @Autowired
    private CuratorFramework curator; //should be pulled in by configuring the spring-cloud-zookeeper-core artifact

    @Value("#{ @environment['samza.job.name'] ?: (@environment['spring.application.name'] ?: null) }")
    private String samzaJobName;

    @Value("#{ @environment['samza.zookeeper.containers.namespace.prefix'] ?: (@environment['samza.zookeper.jobs.namespace'] ?: '/samza/jobs') }")
    private String samzaContainerMembershipPathPrefix;

    @Value("#{ @environment['samza.zookeeper.containers.namespace.suffix'] ?: '/containers' }")
    private String samzaContainerMembershipPathSuffix;

    @Bean
    @ConditionalOnMissingBean(name = "samzaContainerMembershipPath")
    public String samzaContainerMembershipPath() {
        Assert.hasText(samzaJobName, "samza.job.name or spring.application.name must be defined.");
        Assert.hasText(samzaContainerMembershipPathPrefix,
            "samza.zookeeper.containers.namespace.prefix cannot be a null or empty string.");
        Assert.hasText(samzaContainerMembershipPathSuffix,
            "samza.zookeeper.containers.namespace.suffix cannot be a null or empty string.");

        String val = samzaContainerMembershipPathPrefix;
        if (!val.endsWith("/")) {
            val += "/";
        }
        val += samzaJobName;
        if (!samzaContainerMembershipPathSuffix.startsWith("/")) {
            val += "/";
        }
        val += samzaContainerMembershipPathSuffix;

        return val;
    }

    @Bean(initMethod = "start", destroyMethod = "close")
    @ConditionalOnMissingBean(name = "samzaContainerGroupMember")
    public SequentialGroupMember samzaContainerGroupMember() {
        String membersBasePath = samzaContainerMembershipPath();
        String locksBasePath = membersBasePath + "-locks";
        return new SequentialGroupMember(curator, membersBasePath, locksBasePath, EMPTY_PAYLOAD);
    }

    @Bean
    @ConditionalOnMissingBean(name = "samzaContainerCount")
    public int samzaContainerId() {
        SequentialGroupMember member = samzaContainerGroupMember();
        return member.getId();
    }
}
