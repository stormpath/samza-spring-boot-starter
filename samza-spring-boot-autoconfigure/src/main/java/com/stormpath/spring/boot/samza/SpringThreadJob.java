package com.stormpath.spring.boot.samza;

import org.apache.samza.container.RunLoop;
import org.apache.samza.container.SamzaContainer;
import org.apache.samza.container.SamzaContainer$;
import org.apache.samza.job.ApplicationStatus;
import org.apache.samza.job.StreamJob;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.SmartLifecycle;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;
import org.springframework.util.ReflectionUtils;

import java.lang.reflect.Field;

public class SpringThreadJob implements StreamJob, InitializingBean, ApplicationContextAware, SmartLifecycle {

    private static final Logger log = LoggerFactory.getLogger(SpringThreadJob.class);

    private final Runnable samzaContainer;
    private Thread thread = null;
    private volatile ApplicationStatus status;
    private volatile boolean interrupting = false;

    private int phase = 0;
    private String threadName = getClass().getSimpleName();
    private long startWaitMillis = 2000;
    private long stopWaitMillis = 1000;

    private ApplicationContext applicationContext;

    public SpringThreadJob(Runnable samzaContainer) {
        Assert.notNull(samzaContainer, "SamzaContainer Runnable argument cannot be null.");
        this.samzaContainer = samzaContainer;
    }

    public void setPhase(int phase) {
        this.phase = phase;
    }

    public String getThreadName() {
        return threadName;
    }

    public void setThreadName(String threadName) {
        this.threadName = threadName;
    }

    public long getStartWaitMillis() {
        return startWaitMillis;
    }

    public void setStartWaitMillis(long startWaitMillis) {
        this.startWaitMillis = startWaitMillis;
    }

    public long getStopWaitMillis() {
        return stopWaitMillis;
    }

    public void setStopWaitMillis(long stopWaitMillis) {
        this.stopWaitMillis = stopWaitMillis;
    }

    @Override
    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        this.applicationContext = applicationContext;
    }

    @Override
    public void afterPropertiesSet() throws Exception {
        Assert.isTrue(startWaitMillis >= 0, "startWaitMillis must be greater than or equal to zero.");
        Assert.isTrue(stopWaitMillis >= 0, "stopWaitMillis must be greater than or equal to zero.");
        Assert.hasText(threadName, "threadName cannot be null or empty.");
        Assert.notNull(applicationContext, "applicationContext cannot be null.");
    }

    @Override
    public boolean isAutoStartup() {
        return true;
    }

    @Override
    public void stop(Runnable runnable) {
        try {
            stop();
        } finally {
            runnable.run();
        }
    }

    @Override
    public void start() {

        doStart();

        log.debug("Starting Samza container thread...");

        ApplicationStatus status = this.status;

        if (startWaitMillis > 0) {
            status = waitForStatus(ApplicationStatus.Running, startWaitMillis);
        }

        if (status != ApplicationStatus.Running) {
            String msg = "Unable to start Samza container thread successfully within " + startWaitMillis +
                " milliseconds.  Status: " + status;
            throw new IllegalStateException(msg);
        }

        log.info("Started Samza container thread: {}", status);
    }

    private void doStart() {

        status = ApplicationStatus.New;

        thread = new Thread() {
            @Override
            public void run() {
                try {
                    samzaContainer.run();
                    status = ApplicationStatus.SuccessfulFinish;
                } catch (Exception e) {
                    log.error("Samza container startup failed due to exception: " + e.getMessage(), e);
                    status = ApplicationStatus.UnsuccessfulFinish;
                }
            }
        };

        thread.setName(threadName);
        thread.start();

        status = ApplicationStatus.Running;
    }

    @Override
    public void stop() {
        log.debug("Stopping Samza container...");
        kill();
        if (stopWaitMillis > 0) {
            waitForFinish(stopWaitMillis);
        }
        log.info("Stopped Samza container.");
    }

    @Override
    public boolean isRunning() {
        return status == ApplicationStatus.Running;
    }

    @Override
    public int getPhase() {
        return this.phase;
    }

    @Override
    public StreamJob submit() {
        start();
        return this;
    }

    @Override
    public StreamJob kill() {

        try {
            invokeShutdownNow();
        } catch (Exception e) {
            throw new IllegalStateException("Unable to set samzaContainer.runLoop.shutdownNow = true", e);
        }

        return this;
    }

    private void invokeShutdownNow() {
        try {
            Field field = ReflectionUtils.findField(SamzaContainer.class, "runLoop");
            ReflectionUtils.makeAccessible(field);
            final Object runLoopObject = field.get(samzaContainer);

            ReflectionUtils.doWithFields(RunLoop.class, aField -> {
                ReflectionUtils.makeAccessible(aField);
                aField.setBoolean(runLoopObject, true);
            }, aField -> aField.getName().endsWith("shutdownNow"));
        } catch (Exception e) {
            throw new IllegalStateException("Cannot set samzaContainer.runLoop.shutdownNow field", e);
        }
    }

    @Override
    public ApplicationStatus waitForFinish(long timeoutMs) {

        Assert.isTrue(timeoutMs > 0, "timeoutMs must be greater than zero.");

        try {
            thread.join(timeoutMs);
        } catch (InterruptedException e) {
            throw new IllegalStateException("waitForFinish interrupted.", e);
        }

        return status;
    }

    @Override
    public ApplicationStatus waitForStatus(ApplicationStatus status, long timeoutMs) {

        Assert.isTrue(timeoutMs > 0, "timeoutMs must be greater than zero.");

        long start = System.currentTimeMillis();

        while (System.currentTimeMillis() - start < timeoutMs && !status.equals(this.status)) {
            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
                throw new IllegalStateException("waitForStatus interrupted.", e);
            }
        }

        return this.status;
    }

    @Override
    public ApplicationStatus getStatus() {
        return this.status;
    }
}
