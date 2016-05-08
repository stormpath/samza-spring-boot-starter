package com.stormpath.spring.boot.samza;

import org.springframework.context.ApplicationContext;

public class StaticApplicationContextProvider {

    private static ApplicationContext _applicationContext;

    public static void setApplicationContext(ApplicationContext applicationContext) {
        _applicationContext = applicationContext;
    }

    public static ApplicationContext getApplicationContext() {
        return _applicationContext;
    }

}
