package com.stormpath.spring.boot.samza;

import org.apache.samza.checkpoint.CheckpointManager;
import org.apache.samza.checkpoint.CheckpointManagerFactory;
import org.apache.samza.config.Config;
import org.apache.samza.config.JobConfig$;
import org.apache.samza.config.MapConfig;
import org.apache.samza.config.TaskConfig$;
import org.apache.samza.container.SamzaContainer;
import org.apache.samza.container.SamzaContainer$;
import org.apache.samza.coordinator.JobCoordinator$;
import org.apache.samza.job.model.ContainerModel;
import org.apache.samza.job.model.JobModel;
import org.springframework.beans.factory.BeanInitializationException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.CompositePropertySource;
import org.springframework.core.env.ConfigurableEnvironment;
import org.springframework.core.env.EnumerablePropertySource;
import org.springframework.core.env.PropertySource;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

@Configuration
@ConditionalOnProperty(name = "samza.enabled", matchIfMissing = true)
public class SamzaAutoConfiguration {

    protected static final String SAMZA_PROPERTY_PREFIX = "samza.";

    private static final ThreadLocal<CheckpointManagerFactory> configTimeCheckpointManagerFactory = new ThreadLocal<>();
    private static final ThreadLocal<ApplicationContext> configTimeApplicationContext = new ThreadLocal<>();

    @Autowired
    private ConfigurableEnvironment configurableEnvironment;

    @Autowired
    private ApplicationContext applicationContext;

    /*@Autowired
    private CheckpointManagerFactory checkpointManagerFactory;

    @Autowired
    private CheckpointManager checkpointManager; */

    @Value("#{ @environment['samza.container.count'] ?: 1 }")
    private int samzaContainerCount; //only used for static configuration of the number of job instances

    @Value("#{ @environment['samza.container.id'] ?: 0 }")
    private int samzaContainerId;

    @Value("#{ @environment['samza.job.name'] ?: (@environment['spring.application.name'] ?: '') }")
    private String samzaJobName;

    public static CheckpointManagerFactory getConfigTimeCheckpointManagerFactory() {
        return configTimeCheckpointManagerFactory.get();
    }

    public static ApplicationContext getConfigTimeApplicationContext() {
        return configTimeApplicationContext.get();
    }

    @Bean
    public Map<String, String> samzaConfigurationProperties() {
        Map<String, String> props = new HashMap<>();
        findPropertyNamesStartingWith(configurableEnvironment, SAMZA_PROPERTY_PREFIX).stream()
            .forEach(key -> {
                String value = configurableEnvironment.getProperty(key);
                String unprefixed = key.substring(SAMZA_PROPERTY_PREFIX.length());
                props.put(unprefixed, value);
            });
        return props;
    }

    @Bean
    @ConditionalOnMissingBean
    public CheckpointManager samzaCheckpointManager() {
        return new DisabledCheckpointManager();
    }

    @Bean
    @ConditionalOnMissingBean
    public Config samzaConfig() {

        CheckpointManagerFactory factory = null;

        CheckpointManager manager = samzaCheckpointManager();

        //don't overwrite the original values:
        Map<String, String> props = new HashMap<>();
        props.putAll(samzaConfigurationProperties());

        String key = JobConfig$.MODULE$.STREAM_JOB_FACTORY_CLASS();
        String value = props.get(key);
        if (StringUtils.hasLength(value)) {
            String msg = "The '" + key + "' property is not supported by the Samza Spring Boot Starter.  " +
                "Define a StreamJob bean instead.";
            throw new BeanInitializationException(msg);
        }

        Assert.hasText(samzaJobName, "samza.job.name or spring.application.name must be defined.");
        props.put(JobConfig$.MODULE$.JOB_NAME(), samzaJobName);

        key = TaskConfig$.MODULE$.TASK_CLASS();
        value = props.get(key);
        if (StringUtils.hasLength(value)) {
            String msg = "The '" + key + "' property is not supported by the Samza Spring Boot Starter.  " +
                "Define a prototype-scoped StreamTask bean instead.  It MUST be prototype-scoped.";
            throw new BeanInitializationException(msg);
        }
        configTimeApplicationContext.set(applicationContext); //needed by this next value:
        props.put(key, ConfigTimeStreamTask.class.getCanonicalName());

        //Ensure Spring-configured CheckpointManagerFactory or CheckpointManager is used:
        key = TaskConfig$.MODULE$.CHECKPOINT_MANAGER_FACTORY();
        if (!props.containsKey(key)) {

            if (/*factory == null &&*/ manager != null && (!(manager instanceof DisabledCheckpointManager))) {
                factory = new FixedCheckpointManagerFactory(manager);
            }

            if (factory != null) {
                configTimeCheckpointManagerFactory.set(factory);
                String className = ConfigTimeCheckpointManagerFactory.class.getCanonicalName();
                props.put(key, className);
            }
        }

        return new MapConfig(props);
    }

    @Bean
    @ConditionalOnMissingBean(name = "samzaContainerCount")
    public int samzaContainerCount() {
        Assert.isTrue(samzaContainerCount > 0, "samza.container.count must be a positive integer (greater than zero).");
        return samzaContainerCount; //TODO enable Zookeeper supplier to avoid YARN messiness
    }

    @Bean
    @ConditionalOnMissingBean(name = "samzaContainerId")
    public int samzaContainerId() {
        Assert.isTrue(samzaContainerId >= 0, "samza.container.id must be a non-negative integer (0 or greater)");
        return samzaContainerId; //will be overwritten if zookeeper is enabled
    }

    @Bean
    @ConditionalOnMissingBean
    public JobModel samzaJobModel() {
        Config samzaConfig = samzaConfig();

        int count = samzaContainerCount();
        Assert.isTrue(count > 0, "samzaContainerCount must be a positive integer (greater than zero).");

        return JobCoordinator$.MODULE$.buildJobModel(samzaConfig, count);
    }

    @Bean
    @ConditionalOnMissingBean
    public SamzaContainer samzaContainer() {

        JobModel jobModel = samzaJobModel();

        int containerId = samzaContainerId();
        Assert.isTrue(containerId >= 0, "samzaContainerId must be a non-negative integer (0 or greater).");

        Map<Integer, ContainerModel> containers = jobModel.getContainers();
        ContainerModel containerModel = containers.get(containerId);
        if (containerModel == null) {
            String msg = "Container model does not exist for samza container id '" + containerId + "'.  " +
                "Ensure that samzaContainerId is a zero-based integer less than the total number " +
                "of containers for the job.  Number of containers in the model: " + containers.size();
            throw new BeanInitializationException(msg);
        }

        return SamzaContainer$.MODULE$.apply(containerModel, jobModel);
    }

    protected static Set<String> findPropertyNamesStartingWith(ConfigurableEnvironment env, String prefix) {
        return getAllProperties(env).keySet().stream()
            .filter(key -> key.startsWith(prefix))
            .collect(Collectors.toSet());
    }

    protected static Map<String, Object> getAllProperties(ConfigurableEnvironment aEnv) {
        Map<String, Object> result = new HashMap<>();
        aEnv.getPropertySources().forEach(ps -> addAll(result, getAllProperties(ps)));
        return result;
    }

    protected static Map<String, Object> getAllProperties(PropertySource<?> aPropSource) {
        Map<String, Object> result = new HashMap<>();

        if (aPropSource instanceof CompositePropertySource) {
            CompositePropertySource cps = (CompositePropertySource) aPropSource;
            cps.getPropertySources().forEach(ps -> addAll(result, getAllProperties(ps)));
            return result;
        }

        if (aPropSource instanceof EnumerablePropertySource<?>) {
            EnumerablePropertySource<?> ps = (EnumerablePropertySource<?>) aPropSource;
            Arrays.asList(ps.getPropertyNames()).forEach(key -> result.put(key, ps.getProperty(key)));
            return result;
        }

        return result;
    }

    private static void addAll(Map<String, Object> aBase, Map<String, Object> aToBeAdded) {
        for (Map.Entry<String, Object> entry : aToBeAdded.entrySet()) {
            if (aBase.containsKey(entry.getKey())) {
                continue;
            }
            aBase.put(entry.getKey(), entry.getValue());
        }
    }
}
