package com.stormpath.spring.boot.samza.curator;

import com.stormpath.curator.framework.recipes.nodes.SequentialGroupMember;
import com.stormpath.spring.boot.samza.SamzaAutoConfiguration;
import org.apache.curator.framework.CuratorFramework;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.AutoConfigureAfter;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.util.Assert;

@Configuration
@ConditionalOnProperty(name = {"samza.enabled", "spring.cloud.zookeeper.enabled", "samza.zookeeper.enabled"}, matchIfMissing = true)
@AutoConfigureAfter(SamzaAutoConfiguration.class)
public class CuratorSamzaAutoConfiguration {

    private static final byte[] EMPTY_PAYLOAD = new byte[0];

    @SuppressWarnings("SpringJavaAutowiringInspection")
    @Autowired
    private CuratorFramework curator; //should be pulled in by configuring the spring-cloud-zookeeper-core artifact

    @SuppressWarnings("SpringJavaAutowiringInspection")
    @Autowired
    @Qualifier("samzaJobName")
    private String samzaJobName; //defined in SamzaAutoConfiguration

    @Value("#{ @environment['samza.zookeeper.container.membershipPath.prefix'] ?: (@environment['samza.zookeper.jobs.namespace'] ?: '/samza/jobs') }")
    private String samzaContainerMembershipPathPrefix;

    @Value("#{ @environment['samza.zookeeper.container.membershipPath.suffix'] ?: '/containers' }")
    private String samzaContainerMembershipPathSuffix;

    @Bean
    @ConditionalOnMissingBean(name = "samzaContainerMembershipPath")
    public String samzaContainerMembershipPath() {
        Assert.hasText(samzaJobName, "samzaJobName bean (a String) has not been defined.");
        Assert.hasText(samzaContainerMembershipPathPrefix,
            "samza.zookeeper.container.membershipPath.prefix cannot be a null or empty string.");
        Assert.hasText(samzaContainerMembershipPathSuffix,
            "samza.zookeeper.container.membershipPath.suffix cannot be a null or empty string.");

        return samzaContainerMembershipPathPrefix + samzaJobName + samzaContainerMembershipPathSuffix;
    }

    @Bean(initMethod = "start", destroyMethod = "close")
    @ConditionalOnMissingBean(name = "samzaContainerGroupMember")
    public SequentialGroupMember samzaContainerGroupMember() {
        String path = samzaContainerMembershipPath();
        return new SequentialGroupMember(curator, path, EMPTY_PAYLOAD);
    }

    @Bean
    @ConditionalOnMissingBean(name = "samzaContainerCount")
    public int samzaContainerId() {
        SequentialGroupMember member = samzaContainerGroupMember();
        return member.getId();
    }
}
