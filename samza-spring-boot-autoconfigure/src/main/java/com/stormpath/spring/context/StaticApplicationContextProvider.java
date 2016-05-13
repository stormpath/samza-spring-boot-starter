package com.stormpath.spring.context;

import org.springframework.context.ApplicationContext;

public class StaticApplicationContextProvider {

    private static ApplicationContext applicationContext;

    public static ApplicationContext getApplicationContext() {
        return applicationContext;
    }

    public static void setApplicationContext(ApplicationContext applicationContext) {
        StaticApplicationContextProvider.applicationContext = applicationContext;
    }
}
