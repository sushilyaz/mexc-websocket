package com.suhoi.mexcwebsocket.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

@Configuration
public class ScheduleConfig {
    @Bean(destroyMethod = "shutdown")
    public ScheduledExecutorService drainScheduler() {
        return Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "drain-scheduler");
            t.setDaemon(true);
            return t;
        });
    }
}
