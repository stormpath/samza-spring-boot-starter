package com.stormpath.spring.boot.samza

import org.apache.samza.task.StreamTask
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.context.annotation.Scope

@Configuration
class TestTaskConfiguration {

    @Bean
    @Scope("prototype")
    public StreamTask samzaStreamTask() {
        return new TestTask();
    }
}
