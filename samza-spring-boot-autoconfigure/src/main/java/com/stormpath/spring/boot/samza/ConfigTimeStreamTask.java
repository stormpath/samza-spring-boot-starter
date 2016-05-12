package com.stormpath.spring.boot.samza;

import org.apache.samza.config.Config;
import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.task.*;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.BeanInitializationException;
import org.springframework.context.ApplicationContext;
import org.springframework.util.Assert;

public class ConfigTimeStreamTask implements StreamTask, InitableTask, WindowableTask, ClosableTask {

    private static ApplicationContext APPCTX;

    private StreamTask _delegate;

    private StreamTask getDelegate() {
        if (_delegate == null) {
            try {
                Assert.notNull(APPCTX, "static ApplicationContext cannot be null.");
                _delegate = APPCTX.getBean(StreamTask.class);
            } catch (BeansException e) {
                String msg = "Unable to acquire Samza StreamTask bean.  If you enable the " +
                    "Samza Spring Boot Plugin you must declare a prototype bean that implements the " +
                    StreamTask.class.getCanonicalName() + " interface.  It MUST be prototype-scoped.";
                throw new BeanInitializationException(msg, e);
            }
        }
        return _delegate;
    }

    public static void setApplicationContext(ApplicationContext appCtx) {
        ConfigTimeStreamTask.APPCTX = appCtx;
    }

    @Override
    public void init(Config config, TaskContext context) throws Exception {
        StreamTask task = getDelegate();
        if (task instanceof InitableTask) {
            ((InitableTask) task).init(config, context);
        }
    }

    @Override
    public void process(IncomingMessageEnvelope envelope, MessageCollector collector, TaskCoordinator coordinator) throws Exception {
        getDelegate().process(envelope, collector, coordinator);

    }

    @Override
    public void window(MessageCollector collector, TaskCoordinator coordinator) throws Exception {
        StreamTask task = getDelegate();
        if (task instanceof WindowableTask) {
            ((WindowableTask) task).window(collector, coordinator);
        }
    }

    @Override
    public void close() throws Exception {
        StreamTask task = getDelegate();
        if (task instanceof ClosableTask) {
            ((ClosableTask) task).close();
        }
    }
}
